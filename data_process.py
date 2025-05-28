import pandas as pd
import numpy as np
import json
import os
import openai
import logging
from dotenv import load_dotenv
from concurrent.futures import ProcessPoolExecutor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataStreamProcessor:
    def __init__(self):
        load_dotenv()
        # Remove client initialization from __init__
        self.MAX_TOKENS = 128000
        self.TARGET_TOKEN_USAGE = 0.5
        self.MIN_CHUNK_SIZE = 100
        self.MAX_CHUNK_SIZE = 1000

    def _estimate_tokens_per_row(self, df: pd.DataFrame, selected_columns):
        """Optimized token estimation using NumPy vectorized operations."""
        if df.empty or not selected_columns:
            return self.MAX_TOKENS  # Default to max

        sample_df = df[selected_columns].head(100).astype(str)
        avg_chars = sample_df.map(len).mean(axis=0).mean()
        tokens_per_row = max(1, int(avg_chars / 4))  # Approx. 1 token per 4 chars
        return tokens_per_row

    def _calculate_optimal_chunk_size(self, df: pd.DataFrame, selected_columns):
        """Optimized chunk size calculation."""
        try:
            tokens_per_row = self._estimate_tokens_per_row(df, selected_columns)
            max_rows = int((self.MAX_TOKENS * self.TARGET_TOKEN_USAGE) / max(1, tokens_per_row))
            optimal_chunk_size = min(max(self.MIN_CHUNK_SIZE, max_rows), self.MAX_CHUNK_SIZE)
            return optimal_chunk_size
        except Exception as e:
            logger.error(f"Chunk size error: {str(e)}")
            return self.MIN_CHUNK_SIZE

    @staticmethod
    def _process_chunk(chunk: pd.DataFrame, selected_columns, api_key):
        """Optimized parallel processing: process multiple columns per LLM request."""
        # Create client inside the function
        client = openai.OpenAI(api_key=api_key)
        modifications = []
        corrected_chunk = chunk.copy()

        # Load rules inside the function
        def load_rules(column_name):
            try:
                rule_path = f"rules/{column_name.replace(' ', '_')}_rules.json"
                if os.path.exists(rule_path):
                    with open(rule_path, 'r') as f:
                        return json.load(f)
                return {"rules": {}, "type": "string"}
            except Exception as e:
                logger.error(f"Error loading rules for {column_name}: {str(e)}")
                return {"rules": {}, "type": "string"}

        rules = {col: load_rules(col) for col in selected_columns}

        # Format data for LLM request
        data_dict = chunk[selected_columns].astype(str).to_dict(orient="list")
        user_prompt = json.dumps({
            "columns": selected_columns,
            "rules": rules,
            "data": data_dict
        }, indent=2)

        system_prompt = "You are an enterprise-grade data validation assistant. Maintain data integrity and correct inconsistencies."

        try:
            response = client.chat.completions.create(
                model="gpt-4-turbo",
                messages=[{"role": "system", "content": system_prompt},
                          {"role": "user", "content": user_prompt}],
                temperature=0.1,
                response_format={"type": "json_object"}
            )

            result = json.loads(response.choices[0].message.content)
            if 'corrected_data' in result:
                for col in result['corrected_data']:
                    for idx, value in result['corrected_data'][col].items():
                        corrected_chunk.at[int(idx), col] = value
            if 'modifications' in result:
                modifications.extend(result['modifications'])

        except Exception as e:
            logger.error(f"Error processing chunk: {str(e)}")

        return corrected_chunk, modifications

    def process_data(self, data: pd.DataFrame, selected_columns):
        """Optimized process_data function with multiprocessing."""
        if data.empty:
            raise ValueError("Empty DataFrame")

        if any(col not in data.columns for col in selected_columns):
            raise ValueError("Some selected columns are missing")

        chunk_size = self._calculate_optimal_chunk_size(data, selected_columns)
        chunks = [data.iloc[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

        logger.info(f"Processing {len(chunks)} chunks of size {chunk_size}")

        # Get API key once
        api_key = os.getenv('OPEN_AI_KEY')

        results = []
        with ProcessPoolExecutor() as executor:
            # Pass api_key to each process
            futures = [
                executor.submit(self._process_chunk, chunk, selected_columns, api_key) 
                for chunk in chunks
            ]
            results = [future.result() for future in futures]

        corrected_chunks, all_modifications = zip(*results)
        corrected_data = pd.concat(corrected_chunks)
        return corrected_data, sum(all_modifications, [])
