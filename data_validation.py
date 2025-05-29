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
from typing import Dict, List, Tuple, Any
import dask
from models import SQLQueryGenerator, QueryTracker
from datetime import datetime
import boto3
from azure.storage.blob import BlobServiceClient
import io
from concurrent.futures import ThreadPoolExecutor
 
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
        # Create a client pool for better throughput
        self.client_pool = [AsyncOpenAI(api_key=os.getenv('OPEN_AI_KEY')) for _ in range(4)]  
        self.max_chunk_size = 1000  # Increased chunk size
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.engine = create_engine(self.main_conn_str)
        # Prefetch and cache column rules
        self.column_rules_cache = {}

    async def __aenter__(self):
        return self
 
    async def __aexit__(self, exc_type, exc, tb):
        for client in self.client_pool:
            await client.close()
 
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
            
            # Prefetch all column rules to avoid repeated database calls
            self.prefetch_column_rules(pivoted_data.keys())
            
            return pivoted_data
        except Exception as e:
            logger.error(f"Pivot operation failed: {str(e)}")
            raise
    
    def prefetch_column_rules(self, columns):
        """Prefetch rules for all columns to avoid repeated database calls"""
        logger.info(f"Prefetching rules for {len(columns)} columns")
        sandbox_engine = create_engine("postgresql://postgres:Lakshmeesh@localhost:5432/DQM-sandbox")
        
        # Get all sample data in one query
        with sandbox_engine.connect() as conn:
            column_list = ", ".join(columns)
            if column_list:
                try:
                    query = text(f"SELECT {column_list} FROM {self.table_name} LIMIT 10")
                    result = conn.execute(query)
                    rows = result.fetchall()
                    
                    # Process the result into a dict per column
                    for idx, col in enumerate(columns):
                        self.column_rules_cache[col] = {
                            'sample_data': [str(row[idx]) if row[idx] is not None else 'NULL' for row in rows]
                        }
                        
                        # Load rules from file
                        rules_file = os.path.join("rules", self.file_name.split('.')[0].lower(), f"{col}_rules.json")
                        if os.path.exists(rules_file):
                            with open(rules_file, 'r') as f:
                                self.column_rules_cache[col]['rules'] = json.load(f)
                        else:
                            self.column_rules_cache[col]['rules'] = {'rules': []}
                except Exception as e:
                    logger.error(f"Failed to prefetch rules: {str(e)}")
        
        logger.info(f"Prefetched rules for {len(self.column_rules_cache)} columns")
 
    def chunk_records(self, records: list, chunk_size: int) -> list:
        return [records[i:i + chunk_size] for i in range(0, len(records), chunk_size)]
 
    async def process_chunk(self, chunk: list, column: str, client_idx: int) -> Dict[str, Dict]:
        @tenacity.retry(
            stop=tenacity.stop_after_attempt(3),
            wait=tenacity.wait_exponential(multiplier=0.5, min=0.5, max=2),
            retry=tenacity.retry_if_exception_type(Exception)
        )
        async def api_call():
            # Use a client from the pool
            client = self.client_pool[client_idx % len(self.client_pool)]
            
            rules = self.column_rules_cache.get(column, {}).get('rules', {'rules': []})
            sample_data = self.column_rules_cache.get(column, {}).get('sample_data', [])
            
            prompt = self.construct_prompt(chunk, column, rules)
            start = time.time()
            response = await client.chat.completions.create(
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
            logger.info(f"API call for {column} (chunk #{client_idx}) took {time.time() - start:.2f} seconds")
            raw_content = response.choices[0].message.content
            return raw_content
 
        try:
            raw_response = await api_call()
            return self.parse_llm_response(raw_response)
        except Exception as e:
            logger.error(f"Chunk failed for {column} (chunk #{client_idx}): {str(e)}")
            return {str(record['surrogate_key']): {"corrected_value": record['invalid_value'], "reason": "Cannot correct due to processing failure"}
                    for record in chunk}
 
    def construct_prompt(self, chunk: list, column: str, rules: dict) -> str:
        # Only include first 20 records in the prompt to avoid token limits
        record_sample = chunk[:20]
        invalid_records = '\n'.join([f"- Key: {r['surrogate_key']}, Value: {r['invalid_value']}" for r in record_sample])
        
        # Add count of total records
        total_count = len(chunk)
        if total_count > 20:
            invalid_records += f"\n- ... and {total_count - 20} more records with similar patterns"
            
        today_str = self.today

        # Identify common patterns
        patterns = self._identify_patterns(chunk)
        pattern_info = ""
        if patterns:
            pattern_info = "COMMON PATTERNS DETECTED:\n" + "\n".join([f"- {p}" for p in patterns])

        # Build rule-specific instructions
        rules_str = json.dumps(rules.get('rules', []), indent=2)
        rules_instructions = ""
        for rule in rules.get('rules', []):
            if "date" in rule.lower() or "datetime" in rule.lower():
                rules_instructions += (
                    "- Convert ALL date/datetime values to YYYY-MM-DD format with numerical months (e.g., '02' for February).\n"
                    "- Handle month names (e.g., 'Feb', 'March'), misspellings (e.g., 'jaan' → 'Jan'), and irregular separators (e.g., '=', '()').\n"
                    "- Examples: 'feb-12-2024' → '2024-02-12', '01=12=2024' → '2024-12-01', '2024-23-feb' → '2024-02-23'.\n"
                    "- Remove time components.\n"
                    "- Reject dates after {today_str}.\n"
                )
            if "positive" in rule.lower():
                rules_instructions += "- Ensure numeric values are positive.\n"
            if "uppercase" in rule.lower():
                rules_instructions += "- Convert text to uppercase.\n"
            if "lowercase" in rule.lower():
                rules_instructions += "- Convert text to lowercase.\n"
            if "capitalize" in rule.lower():
                rules_instructions += "- Capitalize text.\n"

        return f"""CRITICAL DATA CORRECTION TASK: You MUST correct ALL invalid data for column '{column}' by strictly applying the provided rules. Your goal is 100% correction rate.

    INVALID RECORDS TO CORRECT:
    {invalid_records}

    {pattern_info}

    DATA QUALITY RULES:
    {rules_str}

    SPECIFIC CORRECTION INSTRUCTIONS:
    {rules_instructions}
    1. Apply ALL rules to EVERY record.
    2. For DATE/DATETIME values:
    - Standardize to YYYY-MM-DD with numerical months (e.g., '02' for Feb, '03' for March).
    - Parse month names, abbreviations, or misspellings (e.g., 'jaan' → 'Jan' → '01').
    - Handle irregular formats (e.g., '01=12=2024', '30(10)2024').
    - Remove time components.
    - Reject dates after {today_str}.
    3. For NUMERIC values:
    - Remove non-numeric characters.
    - Convert negative numbers to positive if rules specify 'positive'.
    4. For TEXT/ID values:
    - Apply case transformations per rules.
    5. Provide a brief reason for each correction, referencing the rule.
    6. Apply corrections to ALL {total_count} records.

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

    Maximize corrections while strictly following rules."""

    def _identify_patterns(self, records):
        """Identify common patterns in the data to help the LLM batch process efficiently"""
        if not records:
            return []
            
        patterns = []
        # Check for date-like values
        date_formats = 0
        numeric_values = 0
        empty_values = 0
        
        for r in records[:50]:  # Check a sample
            val = r['invalid_value']
            if val in ['NULL', '']:
                empty_values += 1
            elif any(c in val for c in ['/', '-', '.']) and any(c.isdigit() for c in val):
                date_formats += 1
            elif any(c.isdigit() for c in val):
                numeric_values += 1
                
        if date_formats > 5:
            patterns.append("Many records appear to be dates with non-standard formats")
        if numeric_values > 5:
            patterns.append("Many records appear to be numeric values with formatting issues")
        if empty_values > 5:
            patterns.append("Many records are empty or NULL")
            
        return patterns
 
    def parse_llm_response(self, content: str) -> Dict[str, Dict]:
        """Parse LLM response into a corrected structure with better type handling."""
        logger.debug(f"Raw LLM response: {content}")
        
        if not content or content.strip() == "":
            logger.warning("Empty response from LLM")
            return {"corrections": {}}
        
        try:
            # Remove code block markers
            content = content.strip().strip('```json').strip('```').strip()
            
            # Extract JSON if embedded
            if not content.strip().startswith('{'):
                import re
                json_match = re.search(r'({.*})', content, re.DOTALL)
                if json_match:
                    content = json_match.group(1)
                    logger.debug(f"Extracted JSON content: {content}")
            
            # Parse JSON
            result = json.loads(content)
            
            # Validate structure
            if not isinstance(result, dict) or "corrections" not in result:
                logger.error(f"Invalid JSON structure, expected 'corrections' key: {result}")
                return {"corrections": {}}
            
            corrections = result["corrections"]
            if not isinstance(corrections, dict):
                logger.error(f"Corrections must be a dictionary: {corrections}")
                return {"corrections": {}}
            
            # Validate and clean each entry
            valid_corrections = {}
            for key, val in corrections.items():
                if not isinstance(val, dict):
                    logger.warning(f"Skipping invalid correction for key {key}: {val}")
                    continue
                    
                if "corrected_value" not in val or "reason" not in val:
                    logger.warning(f"Skipping malformed entry for key {key}: {val}")
                    continue
                
                corrected_value = val["corrected_value"]
                if corrected_value in ["NULL", "null", None, ""]:
                    corrected_value = ""
                
                valid_corrections[key] = {
                    "corrected_value": corrected_value,
                    "reason": val["reason"]
                }
            
            logger.debug(f"Parsed corrections: {valid_corrections}")
            return {"corrections": valid_corrections}
        
        except json.JSONDecodeError as e:
            logger.error(f"JSON parse failed: {str(e)}. Raw content: {content}")
            return {"corrections": {}}
        except Exception as e:
            logger.error(f"Unexpected parse error: {str(e)}. Raw content: {content}")
            return {"corrections": {}}
    
    async def correct_invalid_records(self, pivoted_data: Dict[str, List[Dict]]) -> Dict[str, Dict]:
        """Process records, correct based on rules, and add summary."""
        results = {}
        update_tasks = []  # Store tasks for database updates
        
        # Process all columns in parallel
        column_tasks = []
        for column, records in pivoted_data.items():
            task = self.process_column(column, records)
            column_tasks.append(task)
            
        # Wait for all column corrections to complete
        column_results = await asyncio.gather(*column_tasks)
        
        # Process the results for each column
        for column, result in column_results:
            results[column] = result
            # Start table updates in the background
            update_task = asyncio.create_task(self._update_column_in_table(column, result))
            update_tasks.append(update_task)
            
        # Wait for all update tasks to complete
        if update_tasks:
            await asyncio.gather(*update_tasks)
            
        return results
        
    async def process_column(self, column: str, records: List[Dict]) -> Tuple[str, Dict]:
        """Process a single column of records."""
        logger.info(f"Processing {column} with {len(records)} records")
        
        # Determine optimal batch size based on record count - larger batches for efficiency
        dynamic_batch_size = min(self.max_chunk_size, max(500, len(records) // 10))
        logger.info(f"Batch size for {column}: {dynamic_batch_size}")
       
        chunks = self.chunk_records(records, dynamic_batch_size)
        
        # Process chunks in parallel with throttling
        semaphore = asyncio.Semaphore(8)  # Limit concurrent API calls
        
        async def process_with_semaphore(chunk, idx):
            async with semaphore:
                return await self.process_chunk(chunk, column, idx)
                
        # Process each chunk with its index for client selection
        tasks = [process_with_semaphore(chunk, i) for i, chunk in enumerate(chunks)]
        chunk_results = await asyncio.gather(*tasks)
        
        # Combine results
        corrections = {}
        for result in chunk_results:
            corrections.update(result.get("corrections", {}))
        
        corrected_list = []
        key_to_record = {str(r['surrogate_key']): r for r in records}
        processed_keys = set(corrections.keys())
        
        # Process successfully corrected records
        for key, outcome in corrections.items():
            if key in key_to_record:
                original = key_to_record[key]
                corrected_list.append({
                    "surrogate_key": original['surrogate_key'],
                    "invalid_value": original['invalid_value'],
                    "corrected_value": outcome["corrected_value"],
                    "reason": outcome["reason"]
                })
        
        # Handle any records the LLM didn't process
        unprocessed = [r for k, r in key_to_record.items() if str(k) not in processed_keys]
        if unprocessed:
            # Process them in batches for efficiency
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [
                    executor.submit(self._fallback_correct, record['invalid_value'], column)
                    for record in unprocessed
                ]
                
                for i, future in enumerate(futures):
                    corrected_value, reason = future.result()
                    record = unprocessed[i]
                    corrected_list.append({
                        "surrogate_key": record['surrogate_key'],
                        "invalid_value": record['invalid_value'],
                        "corrected_value": corrected_value,
                        "reason": reason
                    })
        
        result = {
            "records": corrected_list,
            "summary": {
                "invalid_value_count": len(records),
                "corrected_count": len(corrected_list)
            }
        }
        
        logger.info(f"{column}: {len(corrected_list)} corrected")
        return column, result
 
    def _fallback_correct(self, invalid_value: str, column: str) -> tuple[str, str]:
        """Fallback correction logic based on rules for unprocessed records."""
        rules = self.column_rules_cache.get(column, {}).get('rules', {'rules': []})
        rules_list = rules.get('rules', [])
        
        # Get column data type from the database
        column_type = None
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT data_type FROM information_schema.columns WHERE table_name = '{self.table_name}' AND column_name = '{column}'"))
                column_info = result.fetchone()
                if column_info:
                    column_type = column_info[0].lower()
        except Exception:
            logger.warning(f"Could not determine column type for {column}")
        
        if invalid_value in ["NULL", "", None]:
            return "", "Cannot correct NULL/empty based on rules"
        
        # Check if column is date-like based on name, type, or rules
        is_date_column = (
            column.lower() in ["date", "datetime", "transactiondatetime"] or
            column_type in ["date", "timestamp", "timestamp without time zone"] or
            any("date" in rule.lower() or "datetime" in rule.lower() for rule in rules_list)
        )
        
        if is_date_column:
            try:
                from datetime import datetime
                from dateutil import parser
                today = datetime.strptime(self.today, '%Y-%m-%d')
                
                # Preprocess input
                cleaned_value = invalid_value.lower().strip()
                # Remove time component
                if " " in cleaned_value:
                    cleaned_value = cleaned_value.split(" ")[0]
                # Normalize separators
                for sep in ['=', '(', ')', ':', ';']:
                    cleaned_value = cleaned_value.replace(sep, '-')
                # Fix common misspellings
                month_corrections = {
                    'jaan': 'jan', 'febr': 'feb', 'mar': 'march', 'apr': 'april',
                    'jun': 'june', 'jul': 'july', 'aug': 'august', 'sep': 'sept',
                    'oct': 'october', 'nov': 'november', 'dec': 'december'
                }
                for wrong, correct in month_corrections.items():
                    cleaned_value = cleaned_value.replace(wrong, correct)
                
                # Try parsing with dateutil
                try:
                    date_obj = parser.parse(cleaned_value, dayfirst=False, yearfirst=False)
                    if date_obj.date() > today.date():
                        return invalid_value, "Cannot correct due to future date per rules"
                    corrected_date = date_obj.strftime('%Y-%m-%d')
                    return corrected_date, f"Corrected to {corrected_date} in YYYY-MM-DD format per rules"
                except ValueError:
                    pass
                
                # Try strict formats
                formats_to_try = [
                    '%d-%m-%Y', '%m-%d-%Y', '%Y-%m-%d', 
                    '%d/%m/%Y', '%m/%d/%Y', '%Y/%m/%d', 
                    '%d.%m.%Y', '%m.%d.%Y', '%Y.%m.%d',
                    '%B-%d-%Y', '%b-%d-%Y', '%Y-%d-%B', '%Y-%d-%b'
                ]
                for fmt in formats_to_try:
                    try:
                        date_obj = datetime.strptime(cleaned_value, fmt)
                        if date_obj.date() > today.date():
                            return invalid_value, "Cannot correct due to future date per rules"
                        corrected_date = date_obj.strftime('%Y-%m-%d')
                        return corrected_date, f"Corrected to {corrected_date} in YYYY-MM-DD format per rules"
                    except ValueError:
                        continue
                
                return invalid_value, "Cannot correct due to invalid date format"
            except Exception as e:
                logger.warning(f"Date correction failed for {invalid_value}: {str(e)}")
                return invalid_value, f"Cannot correct date: {str(e)}"
        
        elif column.lower() in ["sales_id", "product"]:
            for rule in rules_list:
                if "uppercase" in rule.lower():
                    return invalid_value.upper(), "Corrected to uppercase per rules"
                if "lowercase" in rule.lower():
                    return invalid_value.lower(), "Corrected to lowercase per rules"
                if "capitalize" in rule.lower():
                    return invalid_value.capitalize(), "Corrected to capitalize per rules"
            return invalid_value.capitalize(), "Corrected to capitalize based on default rules"
        
        elif column_type in ["double precision", "numeric", "decimal", "real", "float"] or \
            column.lower() in ["totalsales", "units_sold", "unit_price", "quantity", "total_sales"]:
            try:
                cleaned_value = ''.join(c for c in invalid_value if c.isdigit() or c in '.-')
                if cleaned_value.count('.') > 1:
                    parts = cleaned_value.split('.')
                    cleaned_value = parts[0] + '.' + ''.join(parts[1:])
                if not cleaned_value or cleaned_value in ['.', '-', '-.']:
                    return "0", "Corrected to 0 due to invalid numeric value"
                try:
                    num = float(cleaned_value)
                    if num < 0 and any("positive" in rule.lower() for rule in rules_list):
                        num = abs(num)
                    return str(num), f"Corrected to {num} per rules"
                except ValueError:
                    return "0", "Corrected to 0 due to invalid numeric value after cleaning"
            except Exception:
                return "0", "Corrected to 0 due to non-numeric value"
        
        elif column_type in ["integer", "bigint", "smallint"]:
            try:
                cleaned_value = ''.join(c for c in invalid_value if c.isdigit() or c == '-')
                if cleaned_value.startswith('-'):
                    cleaned_value = '-' + cleaned_value.lstrip('-')
                if not cleaned_value or cleaned_value == '-':
                    return "0", "Corrected to 0 due to invalid integer value"
                try:
                    num = int(cleaned_value)
                    if num < 0 and any("positive" in rule.lower() for rule in rules_list):
                        num = abs(num)
                    return str(num), f"Corrected to {num} per rules"
                except ValueError:
                    return "0", "Corrected to 0 due to invalid integer value after cleaning"
            except Exception:
                return "0", "Corrected to 0 due to non-integer value"
        
        for rule in rules_list:
            if "uppercase" in rule.lower():
                return invalid_value.upper(), "Corrected to uppercase per rules"
            if "lowercase" in rule.lower():
                return invalid_value.lower(), "Corrected to lowercase per rules"
            if "capitalize" in rule.lower():
                return invalid_value.capitalize(), "Corrected to capitalize per rules"
        
        return invalid_value, "Cannot correct due to no applicable rules"
    
    async def _update_column_in_table(self, column: str, column_result: Dict[str, Any]) -> None:
        """Update a single column in the database in batches with proper type casting."""
        try:
            records = column_result["records"]
            if not records:
                return
                
            # Get column data type from the database
            column_type = None
            try:
                with self.engine.connect() as conn:
                    result = conn.execute(text(f"SELECT data_type FROM information_schema.columns WHERE table_name = '{self.table_name}' AND column_name = '{column}'"))
                    column_info = result.fetchone()
                    if column_info:
                        column_type = column_info[0].lower()
                        logger.info(f"Column {column} has type {column_type}")
            except Exception as e:
                logger.warning(f"Could not determine column type for {column}: {str(e)}")
            
            # Use Dask for high-speed batch updates
            updates_df = pd.DataFrame([
                {"surrogate_key": r["surrogate_key"], "corrected_value": r["corrected_value"]}
                for r in records
            ])
            
            # Split into batches for efficient updates
            batch_size = 500
            batches = [updates_df.iloc[i:i + batch_size] for i in range(0, len(updates_df), batch_size)]
            
            for i, batch in enumerate(batches):
                start = time.time()
                with self.engine.connect() as conn:
                    with conn.begin():
                        # Create temporary table with appropriate type
                        temp_value_type = "TEXT"
                        if column_type in ["double precision", "numeric", "decimal", "real", "float"]:
                            temp_value_type = "DOUBLE PRECISION"
                        elif column_type in ["integer", "bigint", "smallint"]:
                            temp_value_type = "INTEGER"
                        elif column_type in ["date", "timestamp", "timestamp without time zone"]:
                            temp_value_type = "TIMESTAMP"
                        
                        # Create temp table
                        conn.execute(text(f"""
                            CREATE TEMPORARY TABLE temp_updates (
                                corrected_value {temp_value_type},
                                surrogate_key INTEGER
                            ) ON COMMIT DROP
                        """))
                        
                        # Use safe insertion method based on type
                        if temp_value_type in ["DOUBLE PRECISION", "INTEGER"]:
                            for _, row in batch.iterrows():
                                try:
                                    if row["corrected_value"] in ["", "NULL", None]:
                                        # For NULL values, use direct NULL insertion
                                        conn.execute(text(
                                            "INSERT INTO temp_updates (corrected_value, surrogate_key) VALUES (NULL, :surrogate_key)"
                                        ), {"surrogate_key": row["surrogate_key"]})
                                    else:
                                        # For numeric types, use explicit cast in SQL with text concatenation
                                        # This avoids the parameter type casting issue
                                        query = text(f"""
                                            INSERT INTO temp_updates (corrected_value, surrogate_key) 
                                            SELECT CAST(:value AS {temp_value_type}), :surrogate_key
                                        """)
                                        conn.execute(query, {
                                            "value": str(row["corrected_value"]), 
                                            "surrogate_key": row["surrogate_key"]
                                        })
                                except Exception as e:
                                    logger.warning(f"Failed to insert record for {column}: {str(e)}. Inserting NULL instead.")
                                    try:
                                        conn.execute(text(
                                            "INSERT INTO temp_updates (corrected_value, surrogate_key) VALUES (NULL, :surrogate_key)"
                                        ), {"surrogate_key": row["surrogate_key"]})
                                    except Exception as e2:
                                        logger.error(f"Failed to insert NULL fallback for {column}: {str(e2)}")
                        else:
                            # For text and other types, use regular parameterized insertion
                            for _, row in batch.iterrows():
                                try:
                                    conn.execute(text(
                                        "INSERT INTO temp_updates (corrected_value, surrogate_key) VALUES (:value, :surrogate_key)"
                                    ), {"value": row["corrected_value"], "surrogate_key": row["surrogate_key"]})
                                except Exception as e:
                                    logger.warning(f"Failed to insert text record for {column}: {str(e)}")
                        
                        # Update from temp table - this should now work as the temp table has properly cast values
                        try:
                            conn.execute(text(f"""
                                UPDATE {self.table_name}
                                SET {column} = temp_updates.corrected_value
                                FROM temp_updates
                                WHERE {self.table_name}.id = temp_updates.surrogate_key
                            """))
                            logger.info(f"Updated {column} batch {i+1}/{len(batches)}, {len(batch)} records in {time.time() - start:.2f} seconds")
                        except Exception as e:
                            logger.error(f"Failed to update from temp table for {column}: {str(e)}")
                
        except Exception as e:
            logger.error(f"Failed to update {column} in table: {str(e)}")
    
    async def update_original_table(self, successful_validation: Dict[str, Dict]) -> None:
        """This method is now a no-op since updates are handled during correction"""
        logger.info("Table updates already completed during processing")
        return

    def generate_corrected_dataset(self) -> List[Dict]:
        """Fetch all records from the original table and return as a corrected dataset with JSON-safe values."""
        try:
            df = dd.read_sql_table(
                self.table_name,
                self.main_conn_str,
                index_col='id',
                npartitions=50
            )
            
            # Compute the DataFrame
            computed_df = df.compute(scheduler='distributed')
            
            # Replace NaN values with None which will become null in JSON
            # First convert to Python objects to handle mixed types
            for col in computed_df.columns:
                if computed_df[col].dtype.kind in 'fc':  # float or complex types
                    computed_df[col] = computed_df[col].astype(object).where(computed_df[col].notna(), None)
                elif computed_df[col].dtype.kind == 'M':  # datetime types
                    # Convert timestamps to ISO format strings
                    computed_df[col] = computed_df[col].apply(
                        lambda x: x.isoformat() if pd.notna(x) else None
                    )
            
            # Convert to records
            corrected_dataset = json.loads(computed_df.to_json(orient='records', date_format='iso'))
            logger.info(f"Fetched {len(corrected_dataset)} records from {self.table_name} for corrected dataset")
            return corrected_dataset
        except Exception as e:
            logger.error(f"Failed to generate corrected dataset: {str(e)}")
            raise
 
    async def upload_corrected_dataset_to_source(self, new_file_name: str) -> None:
        """Upload the corrected dataset as a CSV back to the original source (AWS S3 or Azure Blob)."""
        try:
            # Fetch the corrected dataset
            corrected_dataset = self.generate_corrected_dataset()
            if not corrected_dataset:
                raise ValueError("No corrected dataset available to upload")

            # Convert to CSV in memory with proper handling of NaN values
            df = pd.DataFrame(corrected_dataset)
            
            # Replace any remaining NaN values with empty strings for CSV output
            df = df.fillna('')
            
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False, na_rep='')
            csv_content = csv_buffer.getvalue().encode('utf-8')
            csv_buffer.close()

            # Upload based on provider (run in a separate thread to not block)
            def upload_to_provider():
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
            
            # Run upload in background
            with ThreadPoolExecutor() as executor:
                executor.submit(upload_to_provider)

        except Exception as e:
            logger.error(f"Failed to upload corrected dataset to {self.provider}: {str(e)}")
            raise
