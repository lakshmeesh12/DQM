from sqlalchemy import create_engine, Column, Integer, String, Text, Float, MetaData, Table, inspect, insert, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
import pandas as pd
import json
import re
import os
import logging
from datetime import datetime, date, timedelta
from typing import Dict, Any, Optional, List, Tuple
from openai import OpenAI
from dotenv import load_dotenv
import numpy as np
from dataclasses import dataclass
from sqlalchemy.ext.declarative import declarative_base
from pathlib import Path
import asyncio
import aiohttp
from sqlalchemy import create_engine, text, inspect, Table, Column, Integer, Text, MetaData
from typing import Optional, List, Dict
from sqlalchemy import text
from typing import Optional, List, Dict
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:Lakshmeesh@localhost:5432/DQM")
OPENAI_API_KEY = os.getenv("OPEN_AI_KEY")
Base = declarative_base()
engine = create_engine(DATABASE_URL)
metadata = MetaData()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
client = OpenAI(api_key=OPENAI_API_KEY)

class DatabaseError(Exception):
    """Custom exception for database operations"""
    pass
@dataclass
class QueryAttempt:
    def __init__(self, column_name: str, rules: dict, sample_data: list, prompt: str, llm_response: str, generated_query: str):
        self.column_name = column_name
        self.rules = rules
        self.sample_data = sample_data
        self.prompt = prompt
        self.llm_response = llm_response
        self.generated_query = generated_query
        self.execution_error = None
        self.execution_result = None
        self.timestamp = datetime.now().isoformat()

class QueryTracker:
    def __init__(self, output_dir: str = "query_logs"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.logger = logging.getLogger("QueryTracker")
        self.logger.setLevel(logging.INFO)
        fh = logging.FileHandler(self.output_dir / "query_generation.log")
        fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(fh)

    def save_attempt(self, attempt: 'QueryAttempt', table_name: str):
        attempt_dir = self.output_dir / table_name / attempt.column_name
        attempt_dir.mkdir(parents=True, exist_ok=True)
        filename = f"attempt_{datetime.fromisoformat(attempt.timestamp).strftime('%Y%m%d_%H%M%S')}.json"
        with open(attempt_dir / filename, 'w') as f:
            json.dump({
                'column_name': attempt.column_name,
                'rules': attempt.rules,
                'sample_data': [str(x) for x in attempt.sample_data],
                'prompt': attempt.prompt,
                'llm_response': attempt.llm_response,
                'generated_query': attempt.generated_query,
                'execution_error': attempt.execution_error,
                'execution_result': attempt.execution_result,
                'timestamp': attempt.timestamp
            }, f, indent=2)

    def save_successful_query(self, query: str, table_name: str, column_name: str):
        query_dir = Path("Query")
        table_dir = query_dir / table_name / "filename"
        table_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{column_name}.sql"
        with open(table_dir / filename, 'w') as f:
            f.write(query)
            
class SQLQueryGenerator:
    def __init__(self, sandbox_engine, tracker: Optional[QueryTracker] = None):
        self.sandbox_engine = sandbox_engine
        self.main_engine = create_engine("postgresql://postgres:Lakshmeesh@localhost:5432/DQM")
        self.tracker = tracker or QueryTracker()
        self.metadata = MetaData()

    @staticmethod
    def _extract_sql_query(response: str) -> str:
        markers = [('SELECT', ';'), ('SELECT', '\n\n'), ('SELECT', '--')]
        for start_marker, end_marker in markers:
            try:
                start_idx = response.find(start_marker)
                if start_idx != -1:
                    end_idx = response.find(end_marker, start_idx)
                    if end_idx != -1:
                        return response[start_idx:end_idx + 1 if end_marker == ';' else end_idx]
            except Exception:
                continue
        query_lines = [line.strip() for line in response.split('\n') if line.strip() and 
                       not line.startswith('--') and not line.lower().startswith(('below', 'explanation')) and 
                       not line.startswith('-----')]
        return ' '.join(query_lines)

    @staticmethod
    def _get_column_type(engine, table_name: str, column_name: str) -> str:
        try:
            inspector = inspect(engine)
            columns = inspector.get_columns(table_name)
            for col in columns:
                if col['name'] == column_name:
                    return str(col['type'])
            return 'text'
        except Exception:
            return 'text'

    @staticmethod
    def _process_query(query: str, table_name: str, column_name: str, engine, sample_data: list, rules: dict) -> str:
        if not query.strip():
            return f"SELECT * FROM {table_name} WHERE {column_name} IS NULL"
        column_type = SQLQueryGenerator._get_column_type(engine, table_name, column_name)
        valid_values = None
        for rule in rules.get('rules', []):
            if 'must be one of:' in rule.lower():
                try:
                    valid_values = re.search(r'\[(.*?)\]', rule).group(1).split(', ')
                except:
                    pass
        has_null_check = 'IS NULL' in query.upper()
        has_empty_check = "= ''" in query or 'TRIM' in query.upper()
        has_valid_values = valid_values and any(val in query for val in valid_values)
        if not (has_null_check and has_empty_check):
            base_conditions = [f"{column_name} IS NULL", f"trim({column_name})::text = ''"]
            if valid_values and not has_valid_values:
                values_list = "'" + "', '".join(valid_values) + "'"
                base_conditions.append(f"{column_name} NOT IN ({values_list})")
            if 'WHERE' in query.upper():
                existing_conditions = query[query.upper().find('WHERE') + 5:].strip()
                if existing_conditions:
                    base_conditions.extend([cond.strip() for cond in existing_conditions.split('OR')])
            query = f"SELECT * FROM {table_name} WHERE {' OR '.join(base_conditions)}"
        return query.strip()

    def generate_and_test_query(self, table_name: str, column_name: str, rules: dict, sample_data: list) -> str:
        max_attempts = 5
        attempt = 0
        last_error = None
        
        while attempt < max_attempts:
            self.tracker.logger.info(f"Attempt {attempt + 1}/{max_attempts} for {table_name}.{column_name}")
            try:
                sample_data_str = "\n".join([f"- {str(value)}" for value in sample_data if value is not None])
                prompt = self._build_prompt(table_name, column_name, rules, sample_data_str, last_error, attempt)
                response = client.chat.completions.create(
                    model="o3-mini",
                    messages=[
                        {"role": "system", "content": "You are a PostgreSQL expert. Generate only the SQL query with no explanations."},
                        {"role": "user", "content": prompt}
                    ]
                )
                llm_response = response.choices[0].message.content.strip()
                raw_query = self._extract_sql_query(llm_response)
                processed_query = self._process_query(raw_query, table_name, column_name, 
                                                     self.sandbox_engine, sample_data, rules)
                current_attempt = QueryAttempt(
                    column_name=column_name,
                    rules=rules,
                    sample_data=sample_data,
                    prompt=prompt,
                    llm_response=llm_response,
                    generated_query=processed_query
                )
                self.tracker.logger.info(f"Testing query:\n{processed_query}")
                with self.sandbox_engine.connect() as conn:
                    result = conn.execute(text(processed_query))
                    rows = result.fetchall()
                    current_attempt.execution_result = {'rows_returned': len(rows), 'success': True}
                    self.tracker.save_attempt(current_attempt, table_name)
                    self.tracker.save_successful_query(processed_query, table_name, column_name)
                    return processed_query
            except Exception as e:
                error_msg = f"Error in attempt {attempt + 1}: {str(e)}"
                last_error = error_msg
                self.tracker.logger.error(error_msg)
                attempt += 1
                if attempt == max_attempts:
                    raise ValueError(f"Failed to generate valid query after {max_attempts} attempts: {error_msg}")
        raise ValueError(f"Failed to generate valid query after {max_attempts} attempts")

    def _build_prompt(self, table_name: str, column_name: str, rules: dict, 
                     sample_data_str: str, last_error: Optional[str] = None, attempt: int = 0) -> str:
        column_type = self._get_column_type(self.sandbox_engine, table_name, column_name)
        rules_list = rules.get('rules', [])
        prompt = f"""Generate a PostgreSQL query to find data quality issues in column '{column_name}' of table '{table_name}'.

    Column Type: {column_type}
    Sample Values:
    {sample_data_str}

    Rules to enforce:
    {chr(10).join(f'- {rule}' for rule in rules_list)}

    Additional Context:
    - Column appears to contain {column_type} data
    - Need to find rows that violate any of the above rules
    - Sample data provides context for patterns (e.g., length, format) to enforce rules
    - Focus on converting rules and sample data patterns into SQL conditions

    Requirements:
    - Return full rows using SELECT * FROM {table_name}
    - Handle NULL and empty values explicitly (e.g., IS NULL, TRIM(column) = '')
    - Use proper type casting for {column_type} if needed
    - Combine all conditions with OR
    - Use simple SQL conditions (e.g., IN, NOT NULL, LIKE, regex) based on rules and sample data
    - DO NOT use a CTE or 'stats' unless a rule explicitly requires statistical calculations (e.g., min/max from data)
    - If a rule requires statistics, use 'WITH stats AS (...)' with perfect syntax: no extra parentheses, proper referencing"""

        if last_error and attempt in [1, 2]:
            prompt += f"""\n\nPrevious attempt failed with error: {last_error}
    Error Handling:
    1. Check if the error is due to syntax (e.g., extra ')', missing clauses) or logic (e.g., invalid conditions)
    2. Fix syntax issues:
    - Remove extra parentheses if present
    - Ensure no standalone subqueries without 'WITH stats AS' if stats are used
    3. If stats were used incorrectly:
    - Remove 'stats' references unless a rule explicitly requires it
    - If required, wrap stats in 'WITH stats AS (...)' correctly
    4. Adjust the query to better align with rules and sample data patterns
    Please fix the query while maintaining the rule enforcement."""
        if attempt == 3:
            prompt += f"""\n\nThis is the 4th attempt. Previous attempts failed with error: {last_error}.
    Rule and Error Analysis Required:
    1. Analyze the rules and sample data patterns to identify complexity or ambiguity
    2. Review the previous error to determine if it’s due to:
    - Complex rules not easily convertible to SQL
    - Syntax errors (e.g., extra ')', missing CTE for stats)
    3. Reframe complex rules into simpler, SQL-friendly conditions:
    - Break down vague rules (e.g., 'ensure accuracy') into specific checks (e.g., length, regex)
    - Use sample data patterns to guide simplification (e.g., consistent formats)
    4. If stats are needed (e.g., min/max from data):
    - Use 'WITH stats AS (...)' with perfect syntax: no extra parentheses, proper CROSS JOIN stats
    - Calculate stats from '{table_name}', not hardcoded values
    5. Focus on main patterns (e.g., length, format) that can be easily converted to SQL
    Please generate a query with restructured rules and error fixes."""
        if attempt == 4:
            prompt += f"""\n\nThis is the 5th and final attempt. Previous attempts failed with error: {last_error}.
    Final Rule and Error Resolution:
    1. Determine the intent of each rule and its measurable aspects
    2. Convert rules to concrete SQL conditions using sample data patterns
    3. Simplify any remaining complex conditions:
    - Approximate vague rules with practical checks (e.g., length, regex)
    - Prioritize enforceable patterns over ambiguous ones
    4. If stats are explicitly required by a rule:
    - Use 'WITH stats AS (...)' with impeccable syntax: no extra parentheses, proper referencing
    - Calculate stats directly from '{table_name}'
    5. Ensure syntax perfection: no unmatched parentheses, no undefined references
    Please generate a query with simplified rules and error corrections."""
        return prompt

    def drop_sandbox_table(self, table_name: str) -> bool:
        try:
            with self.sandbox_engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            return True
        except Exception:
            return False

    def create_invalid_records_table(self, table_name: str) -> Table:
        """Create a table in DQM to store invalid records."""
        invalid_table_name = f"invalid_records_{table_name}"
        try:
            with self.main_engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {invalid_table_name}"))
            
            invalid_table = Table(
                invalid_table_name, self.metadata,
                Column('key', Integer, primary_key=True),
                Column('surrogate_key', Integer),  # References original table's id
                Column('invalid_column', Text),    # Column name (mostly text)
                Column('invalid_value', Text)      # Value (cast all to text for flexibility)
            )
            invalid_table.create(self.main_engine)
            logger.info(f"Created table {invalid_table_name} in DQM database")
            return invalid_table
        except Exception as e:
            logger.error(f"Failed to create invalid records table {invalid_table_name}: {str(e)}")
            raise

    def store_invalid_records(self, table_name: str) -> Dict[str, Dict[str, any]]:
        """Execute queries from Query dir and store invalid records in DQM."""
        results = {'successful_executions': {}, 'failed_executions': {}}
        query_dir = Path("Query") / table_name / "filename"
        
        if not query_dir.exists():
            logger.error(f"Query directory {query_dir} does not exist")
            return results
        
        invalid_table = self.create_invalid_records_table(table_name)
        
        for query_file in query_dir.glob("*.sql"):
            column_name = query_file.stem
            try:
                with open(query_file, 'r') as f:
                    query = f.read().strip()
                
                logger.info(f"Executing query for {table_name}.{column_name}:\n{query}")
                with self.main_engine.connect() as conn:
                    result = conn.execute(text(query))
                    rows = list(result.mappings())  # Convert MappingResult to list of dicts
                    affected_rows = len(rows)
                    
                    # Store invalid records
                    invalid_records = [
                        {
                            'surrogate_key': row['id'],
                            'invalid_column': column_name,
                            'invalid_value': str(row[column_name]) if row[column_name] is not None else 'NULL'
                        }
                        for row in rows
                    ]
                    
                    if invalid_records:
                        conn.execute(
                            invalid_table.insert(),
                            invalid_records
                        )
                        conn.commit()
                    
                    results['successful_executions'][column_name] = {
                        'query': query,
                        'affected_rows': affected_rows,
                        'stored_invalid_rows': len(invalid_records)
                    }
                    logger.info(f"Stored {len(invalid_records)} invalid rows for {column_name}")
            
            except Exception as e:
                error_msg = f"Failed to execute/store query for {column_name}: {str(e)}"
                results['failed_executions'][column_name] = {
                    'query': query if 'query' in locals() else None,
                    'error': error_msg
                }
                logger.error(error_msg)
        
        return results

class DynamicTableManager:
    def __init__(self):
        # Main database engine
        self.engine = engine
        # Sandbox database engine
        self.sandbox_engine = create_engine("postgresql://postgres:Lakshmeesh@localhost:5432/DQM-sandbox")
        self.metadata = MetaData()  # Create new metadata instance for each manager
        self.sandbox_metadata = MetaData()  # Create separate metadata for sandbox
        self.query_generator = SQLQueryGenerator(self.sandbox_engine)
        self._Session = sessionmaker(bind=self.engine)
        
    def get_session(self):
        """Create and return a new session"""
        return self._Session()

    def _sanitize_column_name(self, name: str) -> str:
        """Sanitize column names for PostgreSQL"""
        return re.sub(r'[^a-zA-Z0-9_]', '', name).lower()

    def _get_sql_type(self, dtype: str) -> Any:
        """Map pandas dtypes to SQLAlchemy types"""
        type_mapping = {
            'int64': Integer, 'int32': Integer,
            'float64': Float, 'float32': Float,
            'object': Text, 'bool': Integer,
            'datetime64[ns]': Text, 'category': Text,
            'string': Text
        }
        return type_mapping.get(str(dtype), Text)

    def _create_column_list(self, df: pd.DataFrame, generated_columns: Optional[Dict] = None) -> Tuple[List[Column], Dict]:
        """Helper method to create column list and mapping"""
        columns = [Column('id', Integer, primary_key=True)]
        column_mapping = {}
        
        if generated_columns:
            for idx, orig_col in enumerate(df.columns):
                generated_name = generated_columns[str(idx)]
                sanitized_name = self._sanitize_column_name(generated_name)
                column_mapping[orig_col] = sanitized_name
                dtype = self._get_sql_type(str(df[orig_col].dtype))
                columns.append(Column(sanitized_name, dtype))
        else:
            for orig_col in df.columns:
                sanitized_name = self._sanitize_column_name(orig_col)
                column_mapping[orig_col] = sanitized_name
                dtype = self._get_sql_type(str(df[orig_col].dtype))
                columns.append(Column(sanitized_name, dtype))
                
        return columns, column_mapping

    def create_dynamic_table(self, df: pd.DataFrame, file_name: str, generated_columns: Optional[Dict] = None) -> Tuple[Table, Dict]:
        """
        Create identical tables in both main and sandbox databases from DataFrame.
        """
        try:
            base_name = os.path.splitext(file_name)[0]
            table_name = f"data_{self._sanitize_column_name(base_name)}"
            
            # Drop existing tables if they exist in both databases
            with self.engine.begin() as conn:
                conn.execute(text(f'DROP TABLE IF EXISTS {table_name}'))
            with self.sandbox_engine.begin() as conn:
                conn.execute(text(f'DROP TABLE IF EXISTS {table_name}'))

            # Create separate column lists for each table
            main_columns, column_mapping = self._create_column_list(df, generated_columns)
            sandbox_columns, _ = self._create_column_list(df, generated_columns)

            # Create tables in both databases with their own column objects
            main_table = Table(table_name, self.metadata, *main_columns)
            sandbox_table = Table(table_name, self.sandbox_metadata, *sandbox_columns)
            
            main_table.create(self.engine)
            sandbox_table.create(self.sandbox_engine)
            
            # Prepare data for insertion
            df_to_insert = df.copy()
            df_to_insert.rename(columns=column_mapping, inplace=True)
            
            # Insert data into both databases
            with self.engine.begin() as conn:
                df_to_insert.to_sql(table_name, conn, if_exists='append', index=False)
            
            with self.sandbox_engine.begin() as conn:
                df_to_insert.to_sql(table_name, conn, if_exists='append', index=False)
            
            logger.info(f"Created table {table_name} in both main and sandbox databases with columns: {list(column_mapping.values())}")
            return main_table, column_mapping

        except Exception as e:
            logger.error(f"Table creation error: {str(e)}")
            raise DatabaseError(f"Failed to create tables: {str(e)}")
        

    def generate_validation_query(self, table_name: str, column_name: str, rules_file: str) -> str:
        """Generate SQL validation query based on rules file"""
        try:
            if not os.path.exists(rules_file):
                raise FileNotFoundError(f"Rules file not found: {rules_file}")

            with open(rules_file, 'r') as f:
                rules_data = json.load(f)
            
            if not isinstance(rules_data, dict):
                raise ValueError(f"Invalid rules format in {rules_file}. Expected a dictionary.")

            return self.query_generator.generate_query(
                table_name,
                column_name,
                rules_data
            )

        except Exception as e:
            logger.error(f"Validation query generation error: {str(e)}")
            raise ValueError(f"Failed to generate validation query: {str(e)}")

   