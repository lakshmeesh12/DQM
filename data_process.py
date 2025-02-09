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
        
        # Load environment variables
        load_dotenv()
        
        # Get OpenAI API key from environment
        openai_api_key = os.getenv('OPEN_AI_KEY')
        if not openai_api_key:
            raise ValueError("OpenAI API key not found. Please set OPEN_AI_KEY environment variable.")
        
        # Initialize OpenAI client
        self.client = openai.OpenAI(api_key=openai_api_key)
        
    def _load_rules(self, column_name: str) -> Dict:
        """Load DQ rules for a specific column from rules directory"""
        rules_path = os.path.join('rules', f'{column_name}_rules.json')
        with open(rules_path, 'r') as f:
            return json.load(f)
    
    def _process_chunk(self, chunk: pd.DataFrame, columns: List[str]) -> Tuple[pd.DataFrame, List[Dict]]:
        """Process a chunk of data and validate against DQ rules"""
        modifications = []
        corrected_chunk = chunk.copy()
        
        for column in columns:
            try:
                rules = self._load_rules(column)
                
                # Convert chunk data to a more structured format
                chunk_data = chunk[column].to_dict()
                
                # Prepare the prompt for LLM
                prompt = f"""
                You are a data quality validator. Your task is to validate and correct data according to specific rules.
                
                Rules for the column '{column}':
                {json.dumps(rules['rules'], indent=2)}
                
                Column type: {rules['type']}
                
                Please validate and correct the following data:
                {json.dumps(chunk_data, indent=2)}
                
                For each value that doesn't match the rules, provide a correction.
                
                Return your response in this exact JSON format:
                {{
                    "corrected_data": {{
                        "row_index": "corrected_value"
                    }},
                    "modifications": [
                        {{
                            "row": "row_index",
                            "original": "original_value",
                            "corrected": "corrected_value",
                            "reason": "reason_for_change"
                        }}
                    ]
                }}
                
                Only include values that need correction in the corrected_data.
                """
                
                logger.debug(f"Sending prompt for column {column}")
                
                response = self.client.chat.completions.create(
                    model="gpt-4-turbo",
                    messages=[
                        {"role": "system", "content": "You are a data quality validator that returns only valid JSON responses."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.2,
                    response_format={"type": "json_object"}  # Ensure JSON response
                )
                
                response_content = response.choices[0].message.content
                logger.debug(f"Received response for column {column}: {response_content}")
                
                try:
                    result = json.loads(response_content)
                except json.JSONDecodeError as e:
                    logger.error(f"JSON parsing error for column {column}: {str(e)}")
                    logger.error(f"Response content: {response_content}")
                    continue
                
                # Apply corrections
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
        """
        Process the entire dataset in chunks using parallel processing
        Returns: (original_data, corrected_data, modifications)
        """
        try:
            # Validate input data
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
                    executor.submit(self._process_chunk, chunk, selected_columns)
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
            
            # Combine corrected chunks
            corrected_data = pd.concat(corrected_chunks, axis=0)
            
            return data[selected_columns], corrected_data, all_modifications
            
        except Exception as e:
            logger.error(f"Error in process_data: {str(e)}")
            raise