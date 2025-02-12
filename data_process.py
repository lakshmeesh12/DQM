import pandas as pd
import io
from concurrent.futures import ThreadPoolExecutor
import openai
from typing import List, Dict, Tuple
import json
import os
import numpy as np
import logging
from dotenv import load_dotenv

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class DataStreamProcessor:
    def __init__(self):
        load_dotenv()
        self.client = openai.OpenAI(api_key=os.getenv('OPEN_AI_KEY'))
        self.MAX_TOKENS = 128000  # GPT-4 Turbo token limit
        self.TARGET_TOKEN_USAGE = 0.5  # Use 50% of token limit
        self.MIN_CHUNK_SIZE = 100
        self.MAX_CHUNK_SIZE = 1000
        
    def _estimate_tokens_per_row(self, df: pd.DataFrame, selected_columns: List[str]) -> int:
        """Estimate tokens per row based on data types and content."""
        # Handle empty dataframe
        if df.empty or len(selected_columns) == 0:
            return self.MAX_TOKENS  # Return max tokens to force minimum chunk size
            
        total_tokens = 0
        sample_size = min(100, len(df))
        
        # Ensure sample size is at least 1
        if sample_size < 1:
            sample_size = 1
            
        sample_df = df.head(sample_size)
        
        for column in selected_columns:
            dtype = str(df[column].dtype)
            sample_values = sample_df[column].astype(str).values
            
            # Handle empty sample values
            if len(sample_values) == 0:
                continue
                
            # Calculate average characters per value with safety check
            value_lengths = [len(str(v)) for v in sample_values if str(v) != 'nan']
            avg_chars = np.mean(value_lengths) if value_lengths else 1
            
            # Token estimation based on data type
            if 'object' in dtype or 'string' in dtype:
                # Text data: roughly 1 token per 4 characters
                total_tokens += max(1, (avg_chars / 4))
            elif 'int' in dtype or 'float' in dtype:
                # Numeric data: roughly 1 token per value
                total_tokens += 1
            elif 'datetime' in dtype:
                # DateTime: roughly 2 tokens per value
                total_tokens += 2
            else:
                # Default estimation
                total_tokens += max(1, (avg_chars / 4))
        
        # Ensure we return at least 1 token per row
        return max(1, int(total_tokens))

    def _calculate_optimal_chunk_size(self, df: pd.DataFrame, selected_columns: List[str]) -> int:
        """Calculate optimal chunk size based on data characteristics."""
        try:
            tokens_per_row = self._estimate_tokens_per_row(df, selected_columns)
            column_count = len(selected_columns)
            
            # Base chunk size calculation using token limit
            max_rows_by_tokens = int((self.MAX_TOKENS * self.TARGET_TOKEN_USAGE) / max(1, tokens_per_row))
            
            # Adjust based on column count
            if column_count > 50:
                max_rows_by_columns = 5000 // max(1, column_count)
            else:
                max_rows_by_columns = self.MAX_CHUNK_SIZE
            
            # Take the minimum of both constraints
            optimal_chunk_size = min(max_rows_by_tokens, max_rows_by_columns)
            
            # Set minimum and maximum bounds
            optimal_chunk_size = max(self.MIN_CHUNK_SIZE, min(self.MAX_CHUNK_SIZE, optimal_chunk_size))
            
            logger.info(f"Calculated optimal chunk size: {optimal_chunk_size} rows")
            logger.info(f"Estimated tokens per row: {tokens_per_row}")
            
            return optimal_chunk_size
            
        except Exception as e:
            logger.error(f"Error calculating chunk size: {str(e)}")
            return self.MIN_CHUNK_SIZE

    def _process_chunk(self, chunk: pd.DataFrame, columns: List[str]) -> Tuple[pd.DataFrame, List[Dict]]:
        modifications = []
        corrected_chunk = chunk.copy()
        
        for column in columns:
            try:
                rules = self._load_rules(column)
                column_data = corrected_chunk[column].copy()
                data_dict = column_data.to_dict()
                cleaned_data = self._clean_data_for_json(data_dict)
                
                system_prompt = """You are an enterprise-grade data validation and cleaning assistant specialized in maintaining data integrity, consistency, and compliance with business rules. For every modification, you must provide complete details including index, original value, corrected value, and reasoning."""

                user_prompt = f"""
Analyze and clean the following data for column '{column}' according to these specifications:

COLUMN METADATA:
- Name: {column}
- Type: {rules.get('type', 'string')}
- Records: {len(cleaned_data)}

VALIDATION RULES:
{json.dumps(rules.get('rules', {}), indent=2)}

CLEANING REQUIREMENTS:
1. Data Integrity:
   - Maintain data type consistency
   - Preserve valid relationships
   - Fix sequence breaks
   - Standardize formats

2. Value Processing:
   - Convert negative to positive
   - Fill missing values contextually
   - Remove invalid characters
   - Standardize case formatting

3. Quality Checks:
   - Validate data accuracy
   - Ensure completeness
   - Maintain consistency
   - Format standardization

INPUT DATA:
{json.dumps(cleaned_data, indent=2)}

Return a JSON object strictly following this structure:
{{
    "corrected_data": {{
        "row_index": "corrected_value"
    }},
    "modifications": [
        {{
            "index": "row_index",
            "original_value": "original",
            "corrected_value": "corrected",
            "description": "explanation",
            "rule_applied": "rule"
        }}
    ]
}}"""

                response = self.client.chat.completions.create(
                    model="gpt-4-turbo",
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    temperature=0.1,
                    response_format={"type": "json_object"}
                )
                
                result = json.loads(response.choices[0].message.content)
                
                if 'corrected_data' in result:
                    for idx, value in result['corrected_data'].items():
                        corrected_chunk.at[int(idx), column] = value
                
                if 'modifications' in result:
                    modifications.extend(result['modifications'])
                
            except Exception as e:
                logger.error(f"Error processing column {column}: {str(e)}")
                continue
        
        return corrected_chunk, modifications

    def _clean_data_for_json(self, data: dict) -> dict:
        cleaned_data = {}
        for key, value in data.items():
            if pd.isna(value) or (isinstance(value, float) and (np.isnan(value) or np.isinf(value))):
                cleaned_data[key] = None
            else:
                cleaned_data[key] = str(value) if isinstance(value, (float, int)) else value
        return cleaned_data

    def _load_rules(self, column_name: str) -> Dict:
        try:
            paths = [
                os.path.join('rules', f'{column_name}_rules.json'),
                os.path.join('rules', f'{column_name.replace(" ", "_")}_rules.json')
            ]
            
            for path in paths:
                if os.path.exists(path):
                    with open(path, 'r') as f:
                        return json.load(f)
            
            raise FileNotFoundError(f"Rules not found for column: {column_name}")
            
        except Exception as e:
            logger.error(f"Error loading rules for {column_name}: {str(e)}")
            raise

    def process_data(self, data: pd.DataFrame, selected_columns: List[str]) -> Tuple[pd.DataFrame, List[Dict]]:
        if data.empty:
            raise ValueError("Empty DataFrame")
        
        missing_cols = [col for col in selected_columns if col not in data.columns]
        if missing_cols:
            raise ValueError(f"Columns not found: {missing_cols}")
        
        data = data.replace([np.inf, -np.inf], None)
        working_data = data.copy()
        
        # Calculate optimal chunk size based on data characteristics
        chunk_size = self._calculate_optimal_chunk_size(working_data, selected_columns)
        chunks = [working_data[i:i + chunk_size] for i in range(0, len(working_data), chunk_size)]
        
        logger.info(f"Processing {len(chunks)} chunks of size {chunk_size}")
        results = []
        
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(self._process_chunk, chunk[selected_columns], selected_columns) for chunk in chunks]
            results = [future.result() for future in futures if future.exception() is None]
        
        if not results:
            raise ValueError("No chunks processed successfully")
        
        corrected_chunks, all_modifications = zip(*results)
        corrected_data = pd.concat(corrected_chunks, axis=0)
        corrected_data = corrected_data.where(corrected_data.notna(), None)
        
        return corrected_data, [m for mods in all_modifications for m in mods]