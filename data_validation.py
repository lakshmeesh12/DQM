import logging
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
from sqlalchemy import create_engine, text
import pandas as pd
import os
import json
import asyncio
from openai import AsyncOpenAI
from dotenv import load_dotenv
import time
from functools import lru_cache
import tenacity
from typing import Dict, List
import dask
from models import SQLQueryGenerator, QueryTracker
from datetime import datetime
import boto3
from azure.storage.blob import BlobServiceClient
import io

logger = logging.getLogger(__name__)
load_dotenv()

# Optimized Dask setup
dask.config.set(scheduler='distributed')
cluster = LocalCluster(n_workers=8, threads_per_worker=2, memory_limit='8GB')
client = Client(cluster)

# AWS and Azure Credentials from environment
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_STORAGE_ACCOUNT_SAS_TOKEN = os.getenv("AZURE_STORAGE_ACCOUNT_SAS_TOKEN")

class DataValidator:
    def __init__(self, table_name: str, provider: str, container_name: str, file_name: str):
        self.table_name = table_name
        self.provider = provider.lower()  # "aws" or "azure"
        self.container_name = container_name
        self.file_name = file_name
        self.main_conn_str = "postgresql://postgres:Lakshmeesh@localhost:5432/DQM"
        self.invalid_table_name = f"invalid_records_{table_name}"
        self.client = AsyncOpenAI(api_key=os.getenv('OPEN_AI_KEY'))
        self.max_chunk_size = 500
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.engine = create_engine(self.main_conn_str)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.client.close()

    def read_and_pivot_invalid_records(self) -> Dict[str, List[Dict]]:
        """Efficiently read and pivot invalid records using Dask."""
        try:
            logger.info(f"Reading invalid records table {self.invalid_table_name} from DQM with distributed Dask")
            df = dd.read_sql_table(
                self.invalid_table_name,
                self.main_conn_str,
                index_col='key',
                npartitions=50
            )

            pivoted_data = (
                df.groupby('invalid_column')
                .apply(
                    lambda g: [{'surrogate_key': row['surrogate_key'], 'invalid_value': row['invalid_value']} 
                              for _, row in g.iterrows()],
                    meta=('x', 'object')
                )
                .compute(scheduler='distributed')
                .to_dict()
            )

            logger.info(f"Pivoted data columns: {list(pivoted_data.keys())}")
            return pivoted_data
        except Exception as e:
            logger.error(f"Pivot operation failed: {str(e)}")
            raise

    def chunk_records(self, records: list, chunk_size: int) -> list:
        return [records[i:i + chunk_size] for i in range(0, len(records), chunk_size)]

    async def process_chunk(self, chunk: list, column: str, rules: dict, sample_data: list) -> Dict[str, Dict]:
        @tenacity.retry(
            stop=tenacity.stop_after_attempt(3),
            wait=tenacity.wait_exponential(multiplier=0.5, min=0.5, max=2),
            retry=tenacity.retry_if_exception_type(Exception)
        )
        async def api_call():
            prompt = self.construct_prompt(chunk, column, rules)
            start = time.time()
            response = await self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": """You are an enterprise-grade data validation and correction expert. Your primary mission is to correct ALL data issues according to provided rules. 

    IMPORTANT BEHAVIORS:
    1. You MUST make every reasonable attempt to correct input data
    2. For dates, you MUST normalize to YYYY-MM-DD format, handling various input patterns
    3. For numeric data, you MUST remove non-numeric characters and apply domain rules
    4. You should NEVER leave a record uncorrected unless it's absolutely impossible to fix
    5. You should use pattern recognition to identify common errors and fix them systematically
    6. Your correction strategy should be both rule-based and contextually aware

    REQUIRED OUTPUT:
    Return valid JSON only, no explanatory text outside the JSON structure.
    Focus on maximizing the correction rate while maintaining data integrity."""},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=4096
            )
            logger.info(f"API call for {column} took {time.time() - start:.2f} seconds")
            raw_content = response.choices[0].message.content
            logger.debug(f"Raw LLM response for {column}: {raw_content}")
            return raw_content

        try:
            raw_response = await api_call()
            return self.parse_llm_response(raw_response)
        except Exception as e:
            logger.error(f"Chunk failed for {column}: {str(e)}")
            return {str(record['surrogate_key']): {"corrected_value": record['invalid_value'], "reason": "Cannot correct due to processing failure"} 
                    for record in chunk}

    def construct_prompt(self, chunk: list, column: str, rules: dict) -> str:
        invalid_records = '\n'.join([f"- Key: {r['surrogate_key']}, Value: {r['invalid_value']}" for r in chunk])
        today_str = self.today

        return f"""CRITICAL DATA CORRECTION TASK: You MUST correct ALL invalid data for column '{column}' based on the rules provided. Your goal is 100% correction rate.

    INVALID RECORDS TO CORRECT:
    {invalid_records}

    DATA QUALITY RULES:
    {json.dumps(rules.get('rules', []), indent=2)}

    CORRECTION INSTRUCTIONS:
    1. YOU MUST CORRECT EVERY SINGLE RECORD! This is a business-critical requirement.

    2. For DATE values:
    - ALWAYS convert to YYYY-MM-DD format
    - For DD-MM-YYYY formats (like "11-01-2025"), convert to "2025-01-11"
    - For dates with time (like "11-01-2025 11:52"), extract only the date part and convert
    - Reject only if the date is in the future (after {today_str})
    - Apply these rules strictly across all date entries

    3. For NUMERIC values:
    - Strip any non-numeric characters
    - Make negative numbers positive if rules specify "positive integer"
    - Ensure the result is a valid number that preserves the original intent

    4. For TEXT/ID values:
    - Normalize format according to rules (uppercase, lowercase, capitalize)
    - Preserve the core identifying information

    5. DO NOT leave any record uncorrected unless absolutely impossible to fix.

    6. ALWAYS include a brief reason for each correction.

    EXPECTED RESPONSE FORMAT:
    {{
    "corrections": {{
        "surrogate_key": {{
        "corrected_value": "properly_formatted_value",
        "reason": "brief_explanation_of_correction"
        }},
        ...additional keys...
    }}
    }}

    IMPORTANT: Your performance is measured by how many records you correctly fix. You must maximize your correction rate while following the rules. Do not give up on any record that can be reasonably interpreted and fixed."""

    def parse_llm_response(self, content: str) -> Dict[str, Dict]:
        """Parse LLM response into a corrected structure."""
        if not content or content.strip() == "":
            logger.warning("Empty response from LLM")
            return {"corrections": {}}
        
        try:
            result = json.loads(content)
            if not isinstance(result, dict) or "corrections" not in result:
                logger.error(f"Invalid JSON structure, expected 'corrections': {content}")
                raise ValueError("Response must have a 'corrections' key")
            for key, val in result["corrections"].items():
                if not isinstance(val, dict) or "corrected_value" not in val or "reason" not in val:
                    logger.error(f"Malformed entry in corrections for key {key}: {val}")
                    raise ValueError("Each entry in corrections must have 'corrected_value' and 'reason'")
            return result
        except json.JSONDecodeError as e:
            logger.error(f"JSON parse failed: {str(e)}. Raw content: {content}")
            return {"corrections": {}}
        except Exception as e:
            logger.error(f"Unexpected parse error: {str(e)}. Raw content: {content}")
            return {"corrections": {}}

    async def correct_invalid_records(self, pivoted_data: Dict[str, List[Dict]]) -> Dict[str, Dict]:
        """Process records, correct based on rules, and add summary."""
        results = {}
        semaphore = asyncio.Semaphore(100)

        async def process_chunk_with_semaphore(chunk, column):
            async with semaphore:
                start = time.time()
                result = await self.process_chunk(chunk, column, *self.get_rules_and_sample_data(column))
                logger.info(f"Processed chunk for {column} in {time.time() - start:.2f} seconds")
                return result

        for column, records in pivoted_data.items():
            logger.info(f"Processing {column} with {len(records)} records")
            dynamic_batch_size = min(self.max_chunk_size, max(200, len(records) // 25))
            logger.info(f"Batch size for {column}: {dynamic_batch_size}")
            
            chunks = self.chunk_records(records, dynamic_batch_size)
            tasks = [process_chunk_with_semaphore(chunk, column) for chunk in chunks]
            chunk_results = await asyncio.gather(*tasks)

            corrections = {}
            for result in chunk_results:
                corrections.update(result.get("corrections", {}))

            corrected_list = []
            key_to_record = {str(r['surrogate_key']): r for r in records}
            processed_keys = set(corrections.keys())

            for key, outcome in corrections.items():
                if key in key_to_record:
                    original = key_to_record[key]
                    corrected_list.append({
                        "surrogate_key": original['surrogate_key'],
                        "invalid_value": original['invalid_value'],
                        "corrected_value": outcome["corrected_value"],
                        "reason": outcome["reason"]
                    })

            unprocessed = [r for k, r in key_to_record.items() if str(k) not in processed_keys]
            for record in unprocessed:
                corrected_value, reason = self._fallback_correct(record['invalid_value'], column)
                corrected_list.append({
                    "surrogate_key": record['surrogate_key'],
                    "invalid_value": record['invalid_value'],
                    "corrected_value": corrected_value,
                    "reason": reason
                })

            results[column] = {
                "records": corrected_list,
                "summary": {
                    "invalid_value_count": len(records),
                    "corrected_count": len(corrected_list)
                }
            }
            logger.info(f"{column}: {len(corrected_list)} corrected")

        return results

    def _fallback_correct(self, invalid_value: str, column: str) -> tuple[str, str]:
        """Fallback correction logic based on rules for unprocessed records."""
        rules = self.get_rules_and_sample_data(column)[0]
        if invalid_value in ["NULL", ""]:
            return invalid_value, "Cannot correct NULL/empty based on rules"
        
        if column.lower() == "date":
            try:
                from datetime import datetime
                today = datetime.strptime(self.today, '%Y-%m-%d')
                date_str = invalid_value.split(" ")[0] if " " in invalid_value else invalid_value
                
                date_obj = None
                formats_to_try = ['%d-%m-%Y', '%m-%d-%Y', '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y/%m/%d', '%d.%m.%Y']
                for fmt in formats_to_try:
                    try:
                        date_obj = datetime.strptime(date_str, fmt)
                        corrected_date = date_obj.strftime('%Y-%m-%d')
                        break
                    except ValueError:
                        continue
                
                if date_obj and date_obj <= today:
                    return corrected_date, f"Corrected to {corrected_date} based on YYYY-MM-DD format per rules"
                return invalid_value, "Cannot correct due to invalid or future date per rules"
            except Exception as e:
                return invalid_value, f"Cannot correct date: {str(e)}"
        
        elif column.lower() in ["sales_id", "product"]:
            return invalid_value.capitalize(), "corrected to capitalize based on rules"
        
        elif column.lower() in ["totalsales", "units_sold", "unit_price"]:
            try:
                value = ''.join(filter(str.isdigit, invalid_value))
                if not value:
                    return invalid_value, "Cannot correct due to non-numeric value per rules"
                num = int(value)
                if num < 0 and any("positive integer" in r.lower() for r in rules.get("rules", [])):
                    num = abs(num)
                    return str(num), f"corrected to {num} as positive integer per rules"
                return str(num), f"corrected to {num} by removing special characters per rules"
            except ValueError:
                return invalid_value, "Cannot correct due to non-numeric value per rules"
        
        return invalid_value, "Cannot correct due to no applicable rules"

    @lru_cache(maxsize=200)
    def get_rules_and_sample_data(self, column_name: str) -> tuple:
        try:
            sandbox_engine = create_engine("postgresql://postgres:Lakshmeesh@localhost:5432/DQM-sandbox")
            rules_file = os.path.join("rules", self.file_name.split('.')[0].lower(), f"{column_name}_rules.json")
            rules = json.load(open(rules_file)) if os.path.exists(rules_file) else {'rules': []}
            
            with sandbox_engine.connect() as conn:
                start = time.time()
                result = conn.execute(text(f"SELECT {column_name} FROM {self.table_name} LIMIT 10"))
                sample_data = [str(row[0]) if row[0] is not None else 'NULL' for row in result]
                logger.info(f"Sample data fetch for {column_name} took {time.time() - start:.2f} seconds")
            
            return rules, sample_data
        except Exception as e:
            logger.error(f"Rules/sample failed for {column_name}: {str(e)}")
            return {'rules': []}, []

    def update_original_table(self, successful_validation: Dict[str, Dict]) -> None:
        """Update the original table with corrected values using surrogate_key."""
        try:
            with self.engine.connect() as conn:
                with conn.begin():
                    for column, data in successful_validation.items():
                        for record in data["records"]:
                            surrogate_key = record["surrogate_key"]
                            corrected_value = record["corrected_value"]
                            update_query = text(
                                f"UPDATE {self.table_name} SET {column} = :corrected_value WHERE id = :surrogate_key"
                            )
                            conn.execute(
                                update_query,
                                {"corrected_value": corrected_value, "surrogate_key": surrogate_key}
                            )
                            logger.info(f"Updated {column} for surrogate_key {surrogate_key} to {corrected_value}")
        except Exception as e:
            logger.error(f"Failed to update original table: {str(e)}")
            raise

    def generate_corrected_dataset(self) -> List[Dict]:
        """Fetch all records from the original table and return as a corrected dataset."""
        try:
            df = dd.read_sql_table(
                self.table_name,
                self.main_conn_str,
                index_col='id',
                npartitions=50
            )
            corrected_dataset = df.compute(scheduler='distributed').to_dict(orient='records')
            logger.info(f"Fetched {len(corrected_dataset)} records from {self.table_name} for corrected dataset")
            return corrected_dataset
        except Exception as e:
            logger.error(f"Failed to generate corrected dataset: {str(e)}")
            raise

    def upload_corrected_dataset_to_source(self, new_file_name: str) -> None:
        """Upload the corrected dataset as a CSV back to the original source (AWS S3 or Azure Blob)."""
        try:
            # Fetch the corrected dataset
            corrected_dataset = self.generate_corrected_dataset()
            if not corrected_dataset:
                raise ValueError("No corrected dataset available to upload")

            # Convert to CSV in memory
            df = pd.DataFrame(corrected_dataset)
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_content = csv_buffer.getvalue().encode('utf-8')
            csv_buffer.close()

            # Upload based on provider
            if self.provider == "aws":
                s3 = boto3.client(
                    "s3",
                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
                )
                s3.put_object(
                    Bucket=self.container_name,
                    Key=new_file_name,
                    Body=csv_content,
                    ContentType='text/csv'
                )
                logger.info(f"Uploaded {new_file_name} to AWS S3 bucket {self.container_name}")

            elif self.provider == "azure":
                blob_service_client = BlobServiceClient(
                    f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net?{AZURE_STORAGE_ACCOUNT_SAS_TOKEN}"
                )
                blob_client = blob_service_client.get_blob_client(container=self.container_name, blob=new_file_name)
                blob_client.upload_blob(csv_content, overwrite=True, content_type='text/csv')
                logger.info(f"Uploaded {new_file_name} to Azure container {self.container_name}")

            else:
                raise ValueError(f"Unsupported provider: {self.provider}")

        except Exception as e:
            logger.error(f"Failed to upload corrected dataset to {self.provider}: {str(e)}")
            raise
