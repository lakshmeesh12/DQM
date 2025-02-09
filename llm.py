import pandas as pd
import openai
import io
import json
import os
import numpy as np
from typing import List, Dict
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
OPENAI_API_KEY = os.getenv("OPEN_AI_KEY")

if not OPENAI_API_KEY:
    raise ValueError("❌ OpenAI API Key is missing! Ensure OPEN_AI_KEY is set in .env.")

client = openai.OpenAI(api_key=OPENAI_API_KEY)

def detect_headers(data: str):
    """
    Detects if a CSV file has headers or not.
    Returns: (bool, pd.DataFrame) - (has_headers, dataframe)
    """
    # First try reading with no header
    df = pd.read_csv(io.StringIO(data), header=None, nrows=5)
    
    first_row = df.iloc[0].astype(str)
    rest_of_rows = df.iloc[1:].astype(str)
    
    # Criteria for header detection:
    # 1. First row should be unique
    # 2. First row should not be mostly numeric
    # 3. Pattern of data types should be different in first row vs rest
    
    has_headers = (
        len(set(first_row)) == len(first_row) and  # All values in first row are unique
        sum(str(x).replace('.', '').isdigit() for x in first_row) / len(first_row) < 0.5 and  # Less than 50% numeric
        any(col.str.contains(r'\d').all() for _, col in rest_of_rows.items())  # At least one column in rest of data is all numeric
    )
    
    return has_headers

def column_name_gen(data: str, model: str = "gpt-4-turbo"):
    """
    Generates meaningful column names if a CSV file lacks headers.
    
    Args:
        data (str): CSV file content as string
        model (str): OpenAI model to use for generation
        
    Returns:
        dict: Dictionary containing column names mapping
    """
    df_no_headers = pd.read_csv(io.StringIO(data), header=None)
    df_no_headers = df_no_headers.dropna(axis=1, how='all')
    actual_column_count = len(df_no_headers.columns)

    # Generate headers using LLM
    response = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "user",
                "content": f"""
                    The dataset has {actual_column_count} columns without headers.
                    Generate exactly {actual_column_count} meaningful column names based on the data.
                    Example data:
                    {df_no_headers.head(5).to_csv(index=False, header=False)}
                    Return JSON format: {{"0": "Column1", "1": "Column2", ...}}
                """,
            }
        ],
        temperature=0.4,
    )

    content = response.choices[0].message.content.strip()
    content = content.replace('```json\n', '').replace('\n```', '')
    return json.loads(content)

def classify_columns(df):
    """
    Classify columns as numerical or textual based on their content.
    """
    classifications = {}
    for column in df.columns:
        # Try to convert to numeric, count success rate
        numeric_count = pd.to_numeric(df[column], errors='coerce').notna().sum()
        total_count = len(df[column].dropna())
        
        # If more than 80% of non-null values are numeric, classify as numeric
        if total_count > 0 and (numeric_count / total_count) > 0.8:
            classifications[column] = 'numeric'
        else:
            classifications[column] = 'text'
    
    return classifications

def generate_column_stats(df, column_name, column_type):
    """
    Generate statistical information for a column.
    """
    stats = {}
    non_null_count = df[column_name].count()
    total_count = len(df)
    
    stats['completeness'] = (non_null_count / total_count) * 100 if total_count > 0 else 0
    stats['unique_count'] = df[column_name].nunique()
    stats['uniqueness_ratio'] = (stats['unique_count'] / non_null_count) * 100 if non_null_count > 0 else 0
    
    if column_type == 'numeric':
        numeric_data = pd.to_numeric(df[column_name], errors='coerce')
        stats.update({
            'min': float(numeric_data.min()) if not pd.isna(numeric_data.min()) else None,
            'max': float(numeric_data.max()) if not pd.isna(numeric_data.max()) else None,
            'mean': float(numeric_data.mean()) if not pd.isna(numeric_data.mean()) else None,
            'std': float(numeric_data.std()) if not pd.isna(numeric_data.std()) else None
        })
    else:
        # Text-specific statistics
        non_empty_strings = df[column_name].astype(str).str.strip().str.len() > 0
        stats['non_empty_ratio'] = (non_empty_strings.sum() / non_null_count) * 100 if non_null_count > 0 else 0
        
    return stats

def generate_dq_rules(data: str, column_info: dict, model: str = "gpt-4-turbo"):
    """
    Generate data quality rules for each column based on the data and DQ dimensions.
    """
    df = pd.read_csv(io.StringIO(data))
    
    # Classify columns
    column_classifications = classify_columns(df)
    
    # Generate stats for each column
    column_stats = {}
    for col_name in df.columns:
        column_stats[col_name] = generate_column_stats(
            df, col_name, 
            column_classifications[col_name]
        )
    
    # Generate rules using LLM
    prompt_content = f"""
    Generate comprehensive data quality rules for each column in the dataset.
    Consider these data quality dimensions:
    - Accuracy
    - Completeness
    - Consistency
    - Validity
    - Timeliness
    - Uniqueness
    - Integrity
    - Relevance
    - Reliability
    - Accessibility
    - Compliance

    Column information:
    {json.dumps(column_info, indent=2)}

    Column classifications and statistics:
    {json.dumps(column_stats, indent=2)}

    Return a JSON object with rules for each column, aligned with the relevant DQ dimensions.
    Format: {{"column_name": {{"rules": [], "type": "numeric/text", "statistics": {{}}, "dq_dimensions": []}}}}
    """

    response = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt_content}],
        temperature=0.4,
    )

    rules = json.loads(response.choices[0].message.content.strip()
                      .replace('```json\n', '').replace('\n```', ''))
    
    # Merge generated rules with statistics
    for column in rules:
        if column in column_stats:
            rules[column]['statistics'] = column_stats[column]
    
    return rules

