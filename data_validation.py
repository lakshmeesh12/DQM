import logging
import dask.dataframe as dd
from sqlalchemy import create_engine
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd
import os
import json
import asyncio
from openai import AsyncOpenAI
from dotenv import load_dotenv
from datetime import datetime
from functools import lru_cache
import dask

logger = logging.getLogger(__name__)

load_dotenv()  # Load .env file

# Configure Dask for multi-core processing
dask.config.set(scheduler='threads')  # Use threads for local multi-core, adjust for distributed if needed

class DataValidator:
    def __init__(self, table_name: str, container_name: str, file_name: str):
        """
        Initialize DataValidator for invalid data processing and correction.
        
        Args:
            table_name (str): Name of the original table (e.g., 'data_header_10k').
            container_name (str): Container name for the data.
            file_name (str): File name (e.g., 'header_10k.csv').
        """
        self.table_name = table_name
        self.container_name = container_name
        self.file_name = file_name
        self.main_conn_str = "postgresql://postgres:Lakshmeesh@localhost:5432/DQM"
        self.tracker = QueryTracker(output_dir=f"query_logs/{file_name.split('.')[0].lower()}")
        self.invalid_table_name = f"invalid_records_{table_name}"
        self.client = AsyncOpenAI(api_key=os.getenv('OPENAI_API_KEY', os.getenv('OPEN_AI_KEY')))  # Async OpenAI client
        self._rules_cache = {}  # Cache for rules
        self._sample_cache = {}  # Cache for sample data

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass  # AsyncOpenAI handles cleanup internally

    def read_and_pivot_invalid_records(self) -> Dict[str, List[Dict]]:
        """
        Read and pivot the invalid records table using Dask, grouping by invalid_column (synchronous for Dask efficiency).
        
        Returns:
            Dict[str, List[Dict]]: Dictionary of {column_name: [{surrogate_key, invalid_value}, ...]}.
        """
        try:
            logger.info(f"Reading invalid records table {self.invalid_table_name} from DQM")
            df_invalid = dd.read_sql_table(
                self.invalid_table_name,
                self.main_conn_str,
                index_col='key',
                npartitions=8  # Increased partitions for faster I/O, adjust based on data size
            )
            
            logger.info("Pivoting invalid records by column")
            grouped = (df_invalid.groupby('invalid_column')
                      .apply(
                          lambda x: x[['surrogate_key', 'invalid_value']].to_dict(orient='records'),
                          meta=('x', 'object')
                      )
                      .compute())
            
            pivoted_data = {col: list(records) for col, records in grouped.items()}
            logger.info(f"Pivoted data: {pivoted_data.keys()}")
            return pivoted_data
        
        except Exception as e:
            logger.error(f"Failed to read/pivot invalid records for {self.table_name}: {str(e)}")
            raise

    @lru_cache(maxsize=100)  # Cache rules and sample data for frequent calls
    def get_rules_and_sample_data(self, column_name: str) -> tuple[dict, list]:
        """
        Fetch DQ rules and sample data for a specific column from the sandbox or rules files (synchronous for simplicity, cached).
        
        Args:
            column_name (str): Name of the column (e.g., 'date').
        
        Returns:
            tuple[dict, list]: (rules, sample_data).
        """
        try:
            sandbox_engine = create_engine("postgresql://postgres:Lakshmeesh@localhost:5432/DQM-sandbox")
            query_generator = SQLQueryGenerator(sandbox_engine, self.tracker)
            
            rules_file = os.path.join("rules", self.file_name.split('.')[0].lower(), f"{column_name}_rules.json")
            if os.path.exists(rules_file):
                with open(rules_file, 'r') as f:
                    rules = json.load(f)
            else:
                rules = {'rules': []}
            
            with sandbox_engine.connect() as conn:
                result = conn.execute(text(f"SELECT {column_name} FROM {self.table_name} LIMIT 10"))
                sample_data = [str(row[0]) if row[0] is not None else 'NULL' for row in result]
            
            return rules, sample_data
        
        except Exception as e:
            logger.error(f"Failed to get rules/sample data for {column_name}: {str(e)}")
            return {'rules': []}, []

    async def correct_invalid_records(self, pivoted_data: Dict[str, List[Dict]]) -> Dict[str, Dict[str, Dict]]:
        """
        Send pivoted invalid records, rules, and sample data to LLM for correction asynchronously, returning corrections and uncorrectable data.
        
        Returns:
            Dict[str, Dict[str, Dict]]: {column_name: {'corrections': {surrogate_key: {'value': corrected_value, 'reason': reason}}, 'uncorrectable_data': {surrogate_key: {'value': invalid_value, 'reason': reason}}}}.
        """
        corrected_data = {}
        
        async def correct_column(column: str, records: List[Dict]) -> Dict[str, Dict]:
            if not records:
                logger.info(f"No invalid records for {column}, skipping correction")
                return {'corrections': {}, 'uncorrectable_data': {}}
            
            # Batch records for large columns (e.g., 100 per batch for performance)
            batch_size = 100
            batched_results = {}
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                
                rules, sample_data = self.get_rules_and_sample_data(column)
                
                # Construct advanced, generic prompt for LLM (precise for all types, fast with batches)
                prompt = f"""You are an advanced data validation and correction expert for enterprise-grade applications. Correct the following invalid records for column '{column}' in a generic dataset:

Invalid Records:
{'\n'.join([f"- surrogate_key: {record['surrogate_key']}, invalid_value: {record['invalid_value']}" for record in batch])}

Rules:
{'\n'.join([f"- {rule}" for rule in rules.get('rules', [])])}

Sample Data Patterns (first 10 rows, for pattern analysis only—do not perform mathematical operations like averages or sums):
{'\n'.join([f"- {value}" for value in sample_data])}

Instructions:
- Evaluate each invalid value using the following approach:
  - **Rules Only (Independent Application)**: Apply rules independently when they specify absolute constraints, such as:
    - For datetimes, enforce 'Value should reflect a realistic order date, not being in the future (Timeliness)' by capping at the current date (e.g., {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}) without arbitrary modifications (e.g., do not subtract or add years unless a rule explicitly requires it, like 'must be within X years of a reference date').
    - For format rules (e.g., 'YYYY-MM-DD HH:MM:SS'), correct to match without altering valid components (e.g., year, month, day) unless required by rules.
  - **Rules + Sample Data Patterns + Surrogate Key (Combined Analysis)**: Use sample data patterns and surrogate_key for inference only when rules allow flexibility or require pattern-based correction, such as:
    - For NULL or incomplete values, infer a valid value from sequential patterns (e.g., increments/decrements, common formats) in sample data and surrogate_key (e.g., for IDs, adjust based on sequence starting from sample data, using surrogate_key - 1 for offset, like sample 'VAL001', surrogate_key 9 → 'VAL009').
    - For datetimes, use sample data to infer format (e.g., 'YYYY-MM-DD HH:MM:SS') and recent years, but do not modify years unless rules explicitly require (e.g., cap at today, not adjust arbitrarily).
    - For numerics or text, infer trends (e.g., sequential, format consistency) without math, using surrogate_key for sequential IDs.
  - If a value already matches rules, patterns, and range, keep it unchanged.
  - If you cannot predictably correct a value (e.g., no sequential pattern, no rule-based inference, or no trend in sample data combined with surrogate_key), mark it as uncorrectable with a justification (e.g., 'No sequential pattern or surrogate key inference for NULL', 'Random text with no match', 'No trend in sample data for incomplete value', 'Cannot adjust datetime without explicit rule').
- Do not use mathematical operations (e.g., averages, sums) on the sample data, as it represents only the first 10 rows and is not statistically representative.
- Do not arbitrarily modify years, months, or other components unless explicitly required by rules (e.g., 'not being in the future' means capping at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}, not reducing or adding by a fixed amount).
- Return results as: {{corrections: {{surrogate_key: {{value: corrected_value, reason: reason}}}}, uncorrectable_data: {{surrogate_key: {{value: invalid_value, reason: reason}}}}}} in JSON format."""

                try:
                    # Async LLM call using openai AsyncOpenAI
                    response = await self.client.chat.completions.create(
                        model="o3-mini",  # Adjust model as needed
                        messages=[
                            {"role": "system", "content": "You are a PostgreSQL and data quality expert for enterprise applications. Return only JSON with 'corrections' and 'uncorrectable_data' as specified."},
                            {"role": "user", "content": prompt}
                        ]
                    )
                    
                    # Parse response
                    content = response.choices[0].message.content.strip()
                    logger.debug(f"LLM response for {column} (batch {i//batch_size + 1}): {content}")
                    
                    try:
                        # Try parsing as JSON directly
                        result = json.loads(content)
                        batch_corrections = result.get('corrections', {})
                        batch_uncorrectable = result.get('uncorrectable_data', {})
                    except json.JSONDecodeError:
                        # Fallback: Try parsing as plain text if JSON is malformed
                        try:
                            result = json.loads(content.replace("'", '"'))  # Handle single quotes if present
                            batch_corrections = result.get('corrections', {})
                            batch_uncorrectable = result.get('uncorrectable_data', {})
                        except json.JSONDecodeError:
                            logger.error(f"Invalid JSON format in LLM response for {column} (batch {i//batch_size + 1}): {content}")
                            batch_corrections, batch_uncorrectable = {}, {}
                    
                    if not isinstance(batch_corrections, dict) or not isinstance(batch_uncorrectable, dict):
                        logger.warning(f"Invalid corrections/uncorrectable format for {column} (batch {i//batch_size + 1}): {result}")
                        batch_corrections, batch_uncorrectable = {}, {}
                    
                    # Aggregate results
                    batched_results.update({
                        'corrections': {**batched_results.get('corrections', {}), **batch_corrections},
                        'uncorrectable_data': {**batched_results.get('uncorrectable_data', {}), **batch_uncorrectable}
                    })
                
                except Exception as e:
                    logger.error(f"Failed to correct batch {i//batch_size + 1} for {column}: {str(e)}")
                    batched_results = {'corrections': {}, 'uncorrectable_data': {}}
            
            logger.info(f"Corrected {len(batched_results['corrections'])} values, {len(batched_results['uncorrectable_data'])} uncorrectable for {column}")
            return batched_results

        # Run corrections for all columns concurrently
        tasks = [correct_column(column, records) for column, records in pivoted_data.items()]
        results = await asyncio.gather(*tasks)
        
        corrected_results = {}
        for column, result in zip(pivoted_data.keys(), results):
            corrected_results[column] = result
        
        return corrected_results

# Example usage (for testing or later integration):
if __name__ == "__main__":
    import nest_asyncio
    nest_asyncio.apply()  # Allow running async in Jupyter/IPython if needed
    
    validator = DataValidator("data_generic", "local", "data_generic.csv")  # Generic example
    pivoted_data = validator.read_and_pivot_invalid_records()
    
    # Run async correction
    corrected_data = asyncio.run(validator.correct_invalid_records(pivoted_data))
    for column, data in corrected_data.items():
        print(f"Column: {column}")
        print(f"Corrections: {data['corrections']}")
        print(f"Uncorrectable: {data['uncorrectable_data']}")
        print("---")
