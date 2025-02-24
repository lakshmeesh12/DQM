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
from datetime import datetime  # For today's date

logger = logging.getLogger(__name__)
load_dotenv()

# Optimized Dask setup
dask.config.set(scheduler='distributed')
cluster = LocalCluster(n_workers=8, threads_per_worker=2, memory_limit='8GB')
client = Client(cluster)

class DataValidator:
    def __init__(self, table_name: str, container_name: str, file_name: str):
        self.table_name = table_name
        self.container_name = container_name
        self.file_name = file_name
        self.main_conn_str = "postgresql://postgres:Lakshmeesh@localhost:5432/DQM"
        self.invalid_table_name = f"invalid_records_{table_name}"
        self.client = AsyncOpenAI(api_key=os.getenv('OPEN_AI_KEY'))
        self.max_chunk_size = 500  # Maintain high batch size for speed
        self.today = datetime.now().strftime('%Y-%m-%d')  # Current date for timeliness (Feb 23, 2025)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.client.close()

    def read_and_pivot_invalid_records(self) -> Dict[str, List[Dict]]:
        """Efficiently read and pivot invalid records using Dask (original working version)."""
        try:
            logger.info(f"Reading invalid records table {self.invalid_table_name} from DQM with distributed Dask")
            df = dd.read_sql_table(
                self.invalid_table_name,
                self.main_conn_str,
                index_col='key',
                npartitions=50  # Keep as is, fast (<3s)
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
            prompt = self.construct_prompt(chunk, column, rules, sample_data)
            start = time.time()
            response = await self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are an enterprise-grade data quality management expert. Return only valid JSON, no extra text."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=4096  # Ensure ample tokens for larger batches
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
            return {str(record['surrogate_key']): {"status": "uncorrectable", "reason": "Processing failed"} 
                    for record in chunk}

    def construct_prompt(self, chunk: list, column: str, rules: dict, sample_data: list) -> str:
        invalid_records = '\n'.join([f"- Key: {r['surrogate_key']}, Value: {r['invalid_value']}" for r in chunk])
        today_str = self.today  # Use Feb 23, 2025 as today's date

        return f"""Correct or categorize invalid data for column '{column}' as an enterprise-grade data quality management expert, based on rules, sample data patterns, surrogate key, and today’s date ({today_str}).

Invalid Records:
{invalid_records}

Rules:
{json.dumps(rules.get('rules', []), indent=2)}

Sample Valid Data (first 10 rows, pattern only, no ranges):
{json.dumps(sample_data, indent=2)}

Instructions:
- **Priority: Correct Data**: Correct invalid values using:
  - DQ rules as the primary authority (e.g., for 'date', 'YYYY-MM-DD', no future dates after {today_str}, no null/empty, consistent with business calendar).
  - Patterns from sample data (e.g., '2023-01-01' suggests 'YYYY-MM-DD' format, not ranges).
  - Surrogate key for sequential inference (e.g., surrogate_key 100, sample '2023-01-01' at key 1 → infer '2023-01-100' if sequential, but only for IDs or numerics; for dates, use pattern only).
  - For dates, ensure format 'YYYY-MM-DD' unless otherwise specified, and cap at {today_str} (e.g., future dates like '2025-03-01' are uncorrectable, not corrected to today unless pattern/rules dictate).
  - For NULL values, correct only if a clear pattern exists in sample data and surrogate key (e.g., '2023-01-001' for key 1, '2023-01-100' for key 100). Otherwise, mark as uncorrectable.
  - For numeric columns, remove special characters (e.g., '123abc' → '123') if sample data and rules show only numerics, and enforce positivity if rules specify 'positive integer' (e.g., '-123' → '123').
  - Do not use mathematical operations (e.g., averages) or ranges unless explicitly in rules.
  - Do not modify valid values arbitrarily—only correct based on rules, patterns, and surrogate key.

- **Uncorrectable Data**: Mark values as uncorrectable if they cannot be predicted (e.g., NULL in 'date' with no pattern, random text like 'sabcd', future dates after {today_str}, or incomplete values with no trend in sample data and surrogate key).
  - Examples: 'NULL' with no pattern, '05:00' (invalid date format), 'sabcd' with no match, '2025-03-01' (future date after {today_str}), numeric '-123' if rules require positive but no pattern to adjust.

- Do not include a 'nochange' category—categorize every value as either 'corrected' or 'uncorrectable'.

- Return only valid JSON with:
  - "corrections": {{"surrogate_key": {{"value": "corrected_value", "reason": "brief_reason"}}}}
  - "uncorrectable_data": {{"surrogate_key": {{"value": "invalid_value", "reason": "brief_reason"}}}}
  - Ensure reasons are concise (e.g., "corrected to 2023-01-100 based on pattern YYYY-MM-DD and surrogate_key 100", "uncorrectable due to NULL with no pattern", "uncorrectable due to future date 2025-03-01 after {today_str}").
Do not include any extra text outside the JSON structure."""

    def parse_llm_response(self, content: str) -> Dict[str, Dict]:
        """Parse LLM response into corrected/uncorrectable structure."""
        if not content or content.strip() == "":
            logger.warning("Empty response from LLM")
            return {"corrections": {}, "uncorrectable_data": {}}
        
        try:
            result = json.loads(content)
            if not isinstance(result, dict) or not all(k in result for k in ["corrections", "uncorrectable_data"]):
                logger.error(f"Invalid JSON structure, expected {'corrections', 'uncorrectable_data'}: {content}")
                raise ValueError("Response must have 'corrections' and 'uncorrectable_data' keys")
            for data_type in ["corrections", "uncorrectable_data"]:
                for key, val in result[data_type].items():
                    if not isinstance(val, dict) or "value" not in val or "reason" not in val:
                        logger.error(f"Malformed entry in {data_type} for key {key}: {val}")
                        raise ValueError(f"Each entry in {data_type} must have 'value' and 'reason'")
            return result
        except json.JSONDecodeError as e:
            logger.error(f"JSON parse failed: {str(e)}. Raw content: {content}")
            return {"corrections": {}, "uncorrectable_data": {}}
        except Exception as e:
            logger.error(f"Unexpected parse error: {str(e)}. Raw content: {content}")
            return {"corrections": {}, "uncorrectable_data": {}}

    async def correct_invalid_records(self, pivoted_data: Dict[str, List[Dict]]) -> Dict[str, Dict]:
        """Process records, categorize into corrected/uncorrectable, and add summary with optimized performance."""
        results = {}
        semaphore = asyncio.Semaphore(100)  # Maintain high concurrency

        async def process_chunk_with_semaphore(chunk, column):
            async with semaphore:
                start = time.time()
                result = await self.process_chunk(chunk, column, *self.get_rules_and_sample_data(column))
                logger.info(f"Processed chunk for {column} in {time.time() - start:.2f} seconds")
                return result

        for column, records in pivoted_data.items():
            logger.info(f"Processing {column} with {len(records)} records")
            dynamic_batch_size = min(self.max_chunk_size, max(200, len(records) // 25))  # Maintain large, efficient batches
            logger.info(f"Batch size for {column}: {dynamic_batch_size}")
            
            chunks = self.chunk_records(records, dynamic_batch_size)
            tasks = [process_chunk_with_semaphore(chunk, column) for chunk in chunks]
            chunk_results = await asyncio.gather(*tasks)

            # Merge and categorize results with pivoted data
            corrections = {}
            uncorrectable_data = {}
            key_to_record = {str(r['surrogate_key']): r for r in records}  # O(1) lookup

            for result in chunk_results:
                corrections.update(result.get("corrections", {}))
                uncorrectable_data.update(result.get("uncorrectable_data", {}))

            # Build final results, ensuring all records are accounted for
            corrected_list = []
            uncorrectable_list = []
            processed_keys = set(corrections.keys()).union(uncorrectable_data.keys())

            for key, outcome in corrections.items():
                if key in key_to_record:
                    original = key_to_record[key]
                    corrected_list.append({
                        "surrogate_key": original['surrogate_key'],
                        "invalid_value": original['invalid_value'],
                        "corrected": outcome["value"],
                        "reason": outcome["reason"]
                    })

            for key, outcome in uncorrectable_data.items():
                if key in key_to_record:
                    original = key_to_record[key]
                    uncorrectable_list.append({
                        "surrogate_key": original['surrogate_key'],
                        "invalid_value": original['invalid_value'],
                        "uncorrectable value": outcome["value"],
                        "reason": outcome["reason"]
                    })

            # Handle unprocessed records as uncorrectable (no "nochange" category)
            unprocessed = [r for k, r in key_to_record.items() if str(k) not in processed_keys]
            uncorrectable_list.extend({
                "surrogate_key": r['surrogate_key'],
                "invalid_value": r['invalid_value'],
                "uncorrectable value": r['invalid_value'],
                "reason": "Cannot correct due to no pattern, rule, or surrogate key match"
            } for r in unprocessed)

            results[column] = {
                "corrected": corrected_list,
                "uncorrectable": uncorrectable_list
            }
            # Add summary
            results[column]["summary"] = {
                "invalid_value_count": len(records),
                "llm_corrected_count": len(corrected_list),
                "uncorrected_count": len(uncorrectable_list)
            }
            logger.info(f"{column}: {len(corrected_list)} corrected, {len(uncorrectable_list)} uncorrectable")

        return results

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