import pandas as pd
import openai
import io
import json
import os
import numpy as np
from typing import List, Dict
from dotenv import load_dotenv
import re
# Load environment variables
load_dotenv()
OPENAI_API_KEY = os.getenv("OPEN_AI_KEY")

if not OPENAI_API_KEY:
    raise ValueError("❌ OpenAI API Key is missing! Ensure OPEN_AI_KEY is set in .env.")

client = openai.OpenAI(api_key=OPENAI_API_KEY)

def detect_headers(data: str):
    """
    Detects if a CSV file has headers or not.
    Returns: bool - True if headers are present, False otherwise
    """
    try:
        # Read first few rows
        df = pd.read_csv(io.StringIO(data), header=None, nrows=5)
        
        # If less than 2 rows, can't make a reliable determination
        if len(df) < 2:
            return False
        
        first_row = df.iloc[0].astype(str)
        rest_of_rows = df.iloc[1:].astype(str)
        
        # Enhanced checks for data patterns:
        
        # 1. Check for date patterns in first row
        def is_date(value):
            date_patterns = [
                r'\d{2}[-/]\d{2}[-/]\d{4}',  # DD-MM-YYYY or DD/MM/YYYY
                r'\d{4}[-/]\d{2}[-/]\d{2}',  # YYYY-MM-DD or YYYY/MM/DD
                r'\d{2}[-/]\d{2}[-/]\d{4}\s+\d{2}:\d{2}'  # DD-MM-YYYY HH:MM
            ]
            return any(re.match(pattern, str(value)) for pattern in date_patterns)
        
        first_row_dates = sum(is_date(x) for x in first_row)
        rest_rows_dates = sum(is_date(x) for row in rest_of_rows.itertuples(index=False) 
                            for x in row)
        
        # If first row has significantly different date pattern, it's likely data
        dates_ratio_different = (first_row_dates > 0 and 
                               abs(first_row_dates/len(first_row) - 
                                   rest_rows_dates/(len(rest_of_rows) * len(first_row))) < 0.2)
        
        # 2. Enhanced numeric pattern check
        def is_numeric_or_currency(value):
            # Remove currency symbols and commas
            cleaned = str(value).replace('$', '').replace(',', '').strip()
            try:
                float(cleaned)
                return True
            except ValueError:
                return False
        
        first_row_numeric = sum(is_numeric_or_currency(x) for x in first_row)
        rest_rows_numeric = sum(is_numeric_or_currency(x) for row in rest_of_rows.itertuples(index=False) 
                              for x in row)
        
        # If first row has similar numeric pattern to other rows, it's likely data
        numeric_ratio_similar = (first_row_numeric > 0 and 
                               abs(first_row_numeric/len(first_row) - 
                                   rest_rows_numeric/(len(rest_of_rows) * len(first_row))) < 0.2)
        
        # 3. Check for common header words and patterns
        header_patterns = [
            r'^id$', r'^name$', r'^date$', r'^type$', r'^category$',
            r'^amount$', r'^price$', r'^quantity$', r'^total$', r'^description$',
            r'^status$', r'^code$', r'^region$', r'^sales$', r'^revenue$'
        ]
        
        def is_likely_header(value):
            value = str(value).lower().strip()
            # Check if value matches common header patterns
            return any(re.match(pattern, value) for pattern in header_patterns)
        
        header_word_count = sum(is_likely_header(x) for x in first_row)
        
        # 4. Check for consistent data format in subsequent rows
        def get_data_format(value):
            if is_date(value):
                return 'date'
            elif is_numeric_or_currency(value):
                return 'numeric'
            return 'string'
        
        column_formats = {i: [] for i in range(len(df.columns))}
        for idx, row in df.iloc[1:].iterrows():
            for col in range(len(row)):
                column_formats[col].append(get_data_format(row[col]))
        
        consistent_formats = all(
            len(set(formats)) == 1 
            for formats in column_formats.values()
        )
        
        # Combine all checks for final determination
        is_data_row = (
            dates_ratio_different or  # First row has similar date patterns
            numeric_ratio_similar or  # First row has similar numeric patterns
            header_word_count == 0 or  # No common header words found
            not consistent_formats  # Data format inconsistency in subsequent rows
        )
        
        return not is_data_row  # Return False if first row looks like data
        
    except Exception as e:
        logger.error(f"Error in header detection: {str(e)}")
        return False  # Default to no headers on error

def column_name_gen(data: str, model: str = "gpt-4-turbo"):
    """
    Generates meaningful column names if a CSV file lacks headers.
    """
    try:
        # Read data without headers
        df_no_headers = pd.read_csv(io.StringIO(data), header=None)
        df_no_headers = df_no_headers.dropna(axis=1, how='all')
        actual_column_count = len(df_no_headers.columns)

        # Prepare example data with proper formatting
        example_data = df_no_headers.head(5).to_csv(index=False, header=False)

        response = client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": "You are a data analyst tasked with generating appropriate column names for a dataset."
                },
                {
                    "role": "user",
                    "content": f"""
                    The dataset has {actual_column_count} columns without headers.
                    Based on the pattern and content of the data, generate appropriate column names.
                    
                    Example data rows:
                    {example_data}
                    
                    Analyze the data patterns and return column names that match the data types and content.
                    Return only a JSON object with format: {{"0": "column_name1", "1": "column_name2", ...}}
                    
                    Make column names clear and descriptive, using common naming conventions.
                    """
                }
            ],
            temperature=0.4,
            response_format={"type": "json_object"}
        )

        content = response.choices[0].message.content.strip()
        return json.loads(content)
        
    except Exception as e:
        logger.error(f"Error in column name generation: {str(e)}")
        # Fallback to basic column names if generation fails
        return {str(i): f"Column_{i+1}" for i in range(actual_column_count)}

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
