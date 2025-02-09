# data_process.py
import pandas as pd
import io
from concurrent.futures import ThreadPoolExecutor
import openai
from typing import List, Dict, Tuple
import json
import os
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class DataStreamProcessor:
    def __init__(self, chunk_size: int = 1000):
        self.chunk_size = chunk_size
        load_dotenv()
        openai_api_key = os.getenv('OPEN_AI_KEY')
        if not openai_api_key:
            raise ValueError("OpenAI API key not found. Please set OPEN_AI_KEY environment variable.")
        self.client = openai.OpenAI(api_key=openai_api_key)

    def _load_rules(self, column_name: str) -> Dict:
        """Load DQ rules for a specific column from rules directory"""
        try:
            # Try the direct column name first
            rule_path = os.path.join('rules', f'{column_name}_rules.json')
            if os.path.exists(rule_path):
                with open(rule_path, 'r') as f:
                    return json.load(f)
            
            # If not found, try with sanitized name
            sanitized_name = column_name.replace(" ", "_").replace("/", "_").replace("\\", "_")
            rule_path = os.path.join('rules', f'{sanitized_name}_rules.json')
            with open(rule_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading rules for column {column_name}: {str(e)}")
            raise

    def _process_chunk(self, chunk: pd.DataFrame, columns: List[str]) -> Tuple[pd.DataFrame, List[Dict]]:
        """Process a chunk of data and validate against DQ rules"""
        modifications = []
        corrected_chunk = chunk.copy()
        
        for column in columns:
            try:
                rules = self._load_rules(column)
                chunk_data = corrected_chunk[column].to_dict()
                
                prompt = f"""
                You are a data quality validator. Validate and correct data according to these rules:
                
                Rules for column '{column}':
                {json.dumps(rules['rules'], indent=2)}
                
                Column type: {rules['type']}
                
                Data to validate:
                {json.dumps(chunk_data, indent=2)}
                
                Return only a JSON object with format:
                {{
                    "corrected_data": {{"row_index": "corrected_value"}},
                    "modifications": [
                        {{
                            "row": "row_index",
                            "original": "original_value",
                            "corrected": "corrected_value",
                            "reason": "reason_for_change"
                        }}
                    ]
                }}
                """
                
                response = self.client.chat.completions.create(
                    model="gpt-4-turbo",
                    messages=[
                        {"role": "system", "content": "You are a data quality validator that returns only valid JSON responses."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.2,
                    response_format={"type": "json_object"}
                )
                
                result = json.loads(response.choices[0].message.content)
                
                if 'corrected_data' in result:
                    for row_idx, value in result['corrected_data'].items():
                        corrected_chunk.at[int(row_idx), column] = value
                
                if 'modifications' in result:
                    modifications.extend(result['modifications'])
                
            except Exception as e:
                logger.error(f"Error processing column {column}: {str(e)}")
                continue
        
        return corrected_chunk, modifications

    def process_data(self, data: pd.DataFrame, selected_columns: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame, List[Dict]]:
        """Process the entire dataset in chunks"""
        try:
            if data.empty:
                raise ValueError("Input DataFrame is empty")
            
            if not all(col in data.columns for col in selected_columns):
                missing_cols = [col for col in selected_columns if col not in data.columns]
                raise ValueError(f"Columns not found in DataFrame: {missing_cols}")
            
            chunks = [data[i:i + self.chunk_size] for i in range(0, len(data), self.chunk_size)]
            all_modifications = []
            corrected_chunks = []
            
            with ThreadPoolExecutor() as executor:
                futures = [
                    executor.submit(self._process_chunk, chunk[selected_columns], selected_columns)
                    for chunk in chunks
                ]
                
                for future in futures:
                    try:
                        corrected_chunk, modifications = future.result()
                        corrected_chunks.append(corrected_chunk)
                        all_modifications.extend(modifications)
                    except Exception as e:
                        logger.error(f"Error processing chunk: {str(e)}")
                        continue
            
            if not corrected_chunks:
                raise ValueError("No chunks were successfully processed")
            
            corrected_data = pd.concat(corrected_chunks, axis=0)
            return data[selected_columns], corrected_data, all_modifications
            
        except Exception as e:
            logger.error(f"Error in process_data: {str(e)}")
            raise