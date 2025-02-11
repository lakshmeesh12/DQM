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
    def __init__(self, chunk_size: int = 1000):
        self.chunk_size = chunk_size
        load_dotenv()
        self.client = openai.OpenAI(api_key=os.getenv('OPEN_AI_KEY'))

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
        
        chunks = [working_data[i:i + self.chunk_size] for i in range(0, len(working_data), self.chunk_size)]
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