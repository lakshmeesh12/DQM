import pandas as pd
import openai
import io
import json
import os
import numpy as np
from typing import List, Dict, Optional
from dotenv import load_dotenv
import re
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
OPENAI_API_KEY = os.getenv("OPEN_AI_KEY")

if not OPENAI_API_KEY:
    raise ValueError("❌ OpenAI API Key is missing! Ensure OPEN_AI_KEY is set in .env.")

client = openai.OpenAI(api_key=OPENAI_API_KEY)


def detect_headers(data: str) -> bool:
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
        
        # 1. Check for date patterns
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
        
        dates_ratio_different = (first_row_dates > 0 and 
                               abs(first_row_dates/len(first_row) - 
                                   rest_rows_dates/(len(rest_of_rows) * len(first_row))) < 0.2)
        
        # 2. Enhanced numeric pattern check
        def is_numeric_or_currency(value):
            cleaned = str(value).replace('$', '').replace(',', '').strip()
            try:
                float(cleaned)
                return True
            except ValueError:
                return False
        
        first_row_numeric = sum(is_numeric_or_currency(x) for x in first_row)
        rest_rows_numeric = sum(is_numeric_or_currency(x) for row in rest_of_rows.itertuples(index=False) 
                              for x in row)
        
        numeric_ratio_similar = (first_row_numeric > 0 and 
                               abs(first_row_numeric/len(first_row) - 
                                   rest_rows_numeric/(len(rest_of_rows) * len(first_row))) < 0.2)
        
        # 3. Check for common header words
        header_patterns = [
            r'^id$', r'^name$', r'^date$', r'^type$', r'^category$',
            r'^amount$', r'^price$', r'^quantity$', r'^total$', r'^description$',
            r'^status$', r'^code$', r'^region$', r'^sales$', r'^revenue$'
        ]
        
        def is_likely_header(value):
            value = str(value).lower().strip()
            return any(re.match(pattern, value) for pattern in header_patterns)
        
        header_word_count = sum(is_likely_header(x) for x in first_row)
        
        # 4. Check for consistent data format
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
        
        is_data_row = (
            dates_ratio_different or
            numeric_ratio_similar or
            header_word_count == 0 or
            not consistent_formats
        )
        
        return not is_data_row
        
    except Exception as e:
        logger.error(f"Error in header detection: {str(e)}")
        return False
    
def column_name_gen(data: str, model: str = "o3-mini"):
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
            response_format={"type": "json_object"}
        )

        content = response.choices[0].message.content.strip()
        return json.loads(content)
        
    except Exception as e:
        logger.error(f"Error in column name generation: {str(e)}")
        # Fallback to basic column names if generation fails
        return {str(i): f"Column_{i+1}" for i in range(actual_column_count)}

def detect_lookup_columns(data: str, model: str = "o3-mini") -> List[str]:
    """
    Analyzes columns to determine which ones would benefit from lookup tables.
    Returns a list of column names that need lookup tables.
    """
    try:
        df = pd.read_csv(io.StringIO(data))
        
        # Prepare sample data for analysis
        sample_data = {}
        for col in df.columns:
            total_rows = len(df)
            unique_values = df[col].nunique()
            unique_ratio = unique_values / total_rows
            
            # Get sample of unique values and their frequencies
            value_counts = df[col].value_counts().head(10)
            
            sample_data[col] = {
                "unique_count": int(unique_values),
                "unique_ratio": float(unique_ratio),
                "sample_values": value_counts.index.tolist(),
                "value_frequencies": value_counts.to_dict()
            }

        prompt = f"""
        Analyze these columns and determine which ones should have lookup tables.
        A column needs a lookup table if it meets these criteria:
        1. Has a relatively small set of distinct values (low unique ratio)
        2. Contains categorical or predefined options
        3. Values should be restricted to a specific set
        4. Represents attributes like status, category, type, etc.

        Column statistics and samples:
        {json.dumps(sample_data, indent=2)}

        Return a JSON object with format: {{"columns": ["column1", "column2"]}}
        Include only columns that strongly match the criteria for lookup tables.
        """

        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are a data analyst identifying columns that need lookup tables."},
                {"role": "user", "content": prompt}
            ],
            
            response_format={"type": "json_object"}
        )

        result = json.loads(response.choices[0].message.content)
        return result.get("columns", [])

    except Exception as e:
        logger.error(f"Error in lookup column detection: {str(e)}")
        return []

def generate_lookup_tables(data: str, lookup_columns: List[str]) -> Dict[str, List[str]]:
    """
    Creates lookup tables for specified columns based on actual data.
    """
    try:
        df = pd.read_csv(io.StringIO(data))
        lookup_tables = {}

        for column in lookup_columns:
            if column in df.columns:
                # Get unique values, excluding null/empty values
                values = df[column].dropna().astype(str)
                values = values[values.str.strip() != '']
                unique_values = values.unique().tolist()
                
                # Clean and sort values
                cleaned_values = sorted([str(v).strip() for v in unique_values])
                
                if cleaned_values:  # Only add if there are valid values
                    lookup_tables[column] = cleaned_values

        return lookup_tables

    except Exception as e:
        logger.error(f"Error in lookup table generation: {str(e)}")
        return {}

def classify_columns(df):
    """
    Classify columns as numerical or textual based on their content.
    """
    classifications = {}
    for column in df.columns:
        numeric_count = pd.to_numeric(df[column], errors='coerce').notna().sum()
        total_count = len(df[column].dropna())
        
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
        non_empty_strings = df[column_name].astype(str).str.strip().str.len() > 0
        stats['non_empty_ratio'] = (non_empty_strings.sum() / non_null_count) * 100 if non_null_count > 0 else 0
        
    return stats


def validate_lookup_tables(lookup_tables: Dict[str, List[str]], model: str = "o3-mini") -> Dict[str, List[str]]:
    """
    Validate lookup table values against their column names using LLM to ensure relevancy.
    
    Args:
        lookup_tables (Dict[str, List[str]]): Original lookup tables with column names and their values
        model (str): LLM model to use for validation
        
    Returns:
        Dict[str, List[str]]: Validated lookup tables with irrelevant values removed
    """
    if not lookup_tables:
        return {}
    
    validated_tables = {}
    
    for column_name, values in lookup_tables.items():
        # Skip if no values
        if not values:
            continue
            
        # Construct prompt for LLM
        validation_prompt = f"""
        Analyze the relevancy between the column name '{column_name}' and its lookup values.
        
        Values: {json.dumps(values, indent=2)}
        
        Task:
        1. Determine if each value is semantically relevant to what the column name represents
        2. Consider common variations, misspellings, and industry-specific terms
        3. Flag any values that are:
           - Completely unrelated to the column concept
           - Obvious typos or misspellings
           - Nonsensical or invalid entries
        
        Return a JSON object with this structure:
        {{
            "valid_values": ["list of relevant values"],
            "invalid_values": ["list of irrelevant values"],
            "reasoning": "brief explanation of why certain values were flagged as invalid"
        }}
        """
        
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": "You are a data quality expert analyzing the semantic relevancy between column names and their values."},
                    {"role": "user", "content": validation_prompt}
                ],
                response_format={"type": "json_object"}
            )
            
            validation_result = json.loads(response.choices[0].message.content)
            
            # Log invalid values and reasoning for debugging
            if validation_result.get("invalid_values"):
                logger.info(
                    f"Invalid values found for column '{column_name}': "
                    f"{validation_result['invalid_values']} - "
                    f"Reason: {validation_result.get('reasoning')}"
                )
            
            # Only keep valid values for the lookup table
            if validation_result.get("valid_values"):
                validated_tables[column_name] = validation_result["valid_values"]
        
        except Exception as e:
            logger.error(f"Error validating lookup table for column '{column_name}': {str(e)}")
            # Fall back to original values if validation fails
            validated_tables[column_name] = values
            
    return validated_tables

def generate_dq_rules(data: str, column_info: dict, lookup_tables: Optional[Dict[str, List[str]]] = None, model: str = "o3-mini"):
    """
    Generate data quality rules for each column, incorporating lookup table validation if available.
    """
    try:
        df = pd.read_csv(io.StringIO(data))
        row_count = len(df)  # Count rows sent to LLM
        logger.info(f"Rows sent to LLM for DQ rule generation: {row_count}")
        
        # Classify columns
        column_classifications = classify_columns(df)
        
        # Generate stats for each column
        column_stats = {}
        for col_name in df.columns:
            column_stats[col_name] = generate_column_stats(
                df, col_name, 
                column_classifications[col_name]
            )
        
        # Base prompt
        base_prompt = f"""
        Generate comprehensive data quality (DQ) rules for each column in the dataset.
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
        """
        
        # Add lookup table information if available
        if lookup_tables and len(lookup_tables) > 0:
            lookup_prompt = f"""
            Lookup Tables Information:
            {json.dumps(lookup_tables, indent=2)}

            For columns with lookup tables:
            - MUST include a rule ensuring values exist only in the lookup table
            - List the valid values in the rule description
            - Mark these rules as 'Validity' dimension
            - Example rule format: "Values must be one of: [list of valid values]"
            """
            base_prompt += "\n" + lookup_prompt

        base_prompt += """
        Return a JSON object with rules for each column, formatted as:
        {"column_name": {"rules": [], "type": "numeric/text", "statistics": {}, "dq_dimensions": []}}
        """

        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are a data quality expert generating comprehensive DQ rules."},
                {"role": "user", "content": base_prompt}
            ],
        
            response_format={"type": "json_object"}
        )

        rules = json.loads(response.choices[0].message.content)
        
        # Merge generated rules with statistics
        for column in rules:
            if column in column_stats:
                rules[column]['statistics'] = column_stats[column]
        
        return rules

    except Exception as e:
        logger.error(f"Error in DQ rule generation: {str(e)}")
        return {}