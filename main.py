from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from typing import Optional, List
from pydantic import BaseModel
import logging
import pandas as pd
import io
import os
import json
from fastapi import BackgroundTasks
from data_process import DataStreamProcessor
from fastapi.responses import StreamingResponse
import asyncio
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine, inspect
from sqlalchemy.sql import text
from models import DynamicTableManager
from models import SQLQueryGenerator
from models import QueryTracker
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import asyncio
from pathlib import Path
from datetime import time, datetime
import time
import asyncpg.exceptions
from sqlalchemy.ext.asyncio import create_async_engine
from rule import RuleAdd, RuleUpdate, SingleRuleUpdate, add_rule, delete_rule, load_rules
from sse_starlette.sse import EventSourceResponse
from file_manager import (
    list_s3_buckets, list_s3_files, read_s3_file,
    list_azure_containers, list_azure_files, read_azure_file
)
from llm import column_name_gen, detect_headers, generate_dq_rules, detect_lookup_columns, generate_lookup_tables
from llm import validate_lookup_tables
from data_validation import DataValidator
# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="File Processing API",
    description="API for processing files from different storage options",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models
class ColumnSelection(BaseModel):
    selected_columns: List[str]

@app.get("/")
async def root():
    logger.debug("Root endpoint called")
    return {"message": "API is running"}

@app.get("/storage")
async def get_storage_options():
    """Get available storage options"""
    logger.debug("Storage options endpoint called")
    return {
        "options": ["aws", "azure", "local"]
    }

@app.get("/storage/{selected_option}")
async def get_storage_containers(selected_option: str):
    """Get containers/buckets for selected storage"""
    logger.debug(f"Getting containers for {selected_option}")
    try:
        if selected_option == "aws":
            buckets = list_s3_buckets()
            return {"containers": buckets}
        elif selected_option == "azure":
            containers = list_azure_containers()
            return {"containers": containers}
        elif selected_option == "local":
            return {"containers": ["local"]}
        else:
            raise HTTPException(status_code=400, detail="Invalid storage option")
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

def convert_numpy_types(obj):
    """
    Convert numpy types to Python native types for JSON serialization.
    """
    if isinstance(obj, dict):
        return {key: convert_numpy_types(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(item) for item in obj]
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    return obj

@app.post("/storage/{selected_option}/{container_name}/{file_name}")
async def store_data(
    selected_option: str,
    container_name: str,
    file_name: str,
    file: Optional[UploadFile] = File(None)
):
    """
    Process and store data from different storage options with DQ rule generation.
    Handles both cases with and without headers.
    """
    try:
        # Get file content based on storage type
        file_content = None
        if selected_option == "aws":
            file_content = read_s3_file(container_name, file_name)
        elif selected_option == "azure":
            file_content = read_azure_file(container_name, file_name)
        elif selected_option == "local" and file:
            file_content = await file.read()
            file_content = file_content.decode("utf-8")
       
        if not file_content:
            raise HTTPException(status_code=500, detail="No file content was retrieved")

        # Detect headers
        has_header = detect_headers(file_content)
        
        # Generate column names if no headers
        generated_columns = None
        if not has_header:
            generated_columns = column_name_gen(file_content)
            # Convert generated column names to list in correct order
            column_names = [generated_columns[str(i)] for i in range(len(generated_columns))]
            # Create DataFrame with generated column names
            df = pd.read_csv(io.StringIO(file_content), header=None, names=column_names)
        else:
            # Create DataFrame with existing headers
            df = pd.read_csv(io.StringIO(file_content), header=0)

        # Initialize DynamicTableManager and create table
        table_manager = DynamicTableManager()
        table, column_mapping = table_manager.create_dynamic_table(
            df=df,
            file_name=file_name,
            generated_columns=generated_columns if not has_header else None
        )

        # Clean the DataFrame and process for further operations
        df.columns = column_mapping.values()
        df = df.dropna(how='all').reset_index(drop=True)

        # Initialize dq_rules and converted_dq_rules to empty dictionaries
        dq_rules = {}
        converted_dq_rules = {}

        # Detect and generate lookup tables using mapped column names
        try:
            current_df_string = df.to_csv(index=False)
            lookup_columns = detect_lookup_columns(current_df_string)
            initial_lookup_tables = generate_lookup_tables(current_df_string, lookup_columns) if lookup_columns else None
            
            # Add validation step
            if initial_lookup_tables:
                validated_lookup_tables = validate_lookup_tables(initial_lookup_tables)
                logger.info(f"Validated lookup tables: {validated_lookup_tables}")
                
                # Use validated lookup tables for DQ rule generation
                dq_rules = generate_dq_rules(current_df_string, column_mapping, validated_lookup_tables)
            else:
                dq_rules = generate_dq_rules(current_df_string, column_mapping)
                
            # Convert numpy types to native Python types for serialization
            converted_dq_rules = convert_numpy_types(dq_rules)
                
        except Exception as e:
            logger.error(f"Error while generating DQ rules: {str(e)}")
            # Ensure converted_dq_rules is at least an empty dict if there's an error
            converted_dq_rules = convert_numpy_types(dq_rules)

        # Generate statistics for each column
        column_statistics = {}
        for col in df.columns:
            non_null_count = df[col].notna().sum()
            total_count = len(df)
            unique_count = df[col].nunique()
            
            column_statistics[col] = {
                "completeness": (non_null_count / total_count) * 100 if total_count else 0,
                "unique_count": unique_count,
                "uniqueness_ratio": (unique_count / total_count) * 100 if total_count else 0,
                "non_empty_ratio": (df[col].count() / total_count) * 100 if total_count else 0
            }

        # Store rules in JSON files
        base_rules_dir = os.path.abspath("rules")
        file_base_name = file_name.split('.')[0].lower()
        file_rules_dir = os.path.join(base_rules_dir, file_base_name)
        os.makedirs(file_rules_dir, exist_ok=True)
        
        stored_rules_paths = {}
        for col_name, rules in converted_dq_rules.items():
            sanitized_name = DynamicTableManager()._sanitize_column_name(col_name)
            rules_file = os.path.join(file_rules_dir, f"{sanitized_name}_rules.json")
            with open(rules_file, 'w') as f:
                json.dump(rules, f, indent=2)
            stored_rules_paths[sanitized_name] = rules_file

        # Build response data
        response_data = {
            "status": "success",
            "message": f"Data processed for {file_name}",
            "table_name": table.name,
            "rules": converted_dq_rules,
            "type": "text",
            "statistics": convert_numpy_types(column_statistics),  # Convert statistics as well
            "dq_dimensions": ["Validity", "Completeness", "Relevance"],
            "details": {
                "has_headers": has_header,
                "original_columns": list(column_mapping.keys()),
                "sanitized_columns": list(column_mapping.values()),
                "row_count": len(df),
                "processed_at": datetime.now().isoformat(),
                "generated_columns": generated_columns if not has_header else None
            },
            "sandbox_details": {
                "database": "DQM-sandbox",
                "table_name": table.name,
                "status": "created with masked data"
            }
        }

        # Initialize validated_lookup_tables variable to avoid reference errors
        validated_lookup_tables = {}

        # Update the response data to include both original and validated lookup tables
        if 'initial_lookup_tables' in locals() and initial_lookup_tables:
            validated_lookup_tables = locals().get('validated_lookup_tables', {})
            response_data.update({
                "lookup_tables": {
                    "columns": list(validated_lookup_tables.keys()),
                    "tables": validated_lookup_tables,
                    "original_tables": initial_lookup_tables  # Optional: include original for comparison
                }
            })
        
        # Store metadata if needed
        metadata_file = os.path.join(file_rules_dir, "metadata.json")
        metadata = {
            "file_name": file_name,
            "processed_at": datetime.now().isoformat(),
            "column_mapping": column_mapping,
            "has_headers": has_header,
            "generated_columns": generated_columns if not has_header else None,
            "lookup_tables": validated_lookup_tables if 'initial_lookup_tables' in locals() and initial_lookup_tables else {},
            "table_name": table.name
        }
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)

        return JSONResponse(content=response_data)

    except Exception as e:
        logger.error(f"API Error: {str(e)}")
        error_detail = str(e)
        if len(error_detail) > 200:
            error_detail = error_detail[:200] + "..."
        raise HTTPException(500, detail=error_detail)

@app.post("/rules/{file_name}/{column_name}/add-rule")
async def add_rule_endpoint(
    rule_update: SingleRuleUpdate,
    file_name: str = Path(..., description="Name of the file"),
    column_name: str = Path(..., description="Name of the column")
):
    """Add or edit a single rule for a column via UI (+ symbol). User can input a new rule like 'This column should always start with capital letters'."""
    try:
        file_rules_dir = os.path.join('rules', file_name.replace('.', '_'))
        updated_rules = add_rule(file_rules_dir, column_name, rule_update.rule)
        return {
            "status": "success",
            "message": f"Rule added/edited for column {column_name}",
            "rules": updated_rules
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error adding/editing rule: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/rules/{file_name}/{column_name}/delete-rule/{rule_index}")
async def delete_rule_endpoint(
    file_name: str = Path(..., description="Name of the file"),
    column_name: str = Path(..., description="Name of the column"),
    rule_index: int = Path(..., description="Index of the rule to delete")
):
    """Delete a single rule for a column via UI (delete icon). Removes the rule from the rules file."""
    try:
        file_rules_dir = os.path.join('rules', file_name.replace('.', '_'))
        result = delete_rule(file_rules_dir, column_name, rule_index)
        return {
            "status": "success",
            "message": f"Rule deleted for column {column_name}",
            "deleted_rule": result["deleted_rule"],
            "rules": result["rules"]
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error deleting rule: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/rules/{file_name}/{column_name}")
async def get_rules(
    file_name: str = Path(..., description="Name of the file"),
    column_name: str = Path(..., description="Name of the column")
):
    """Get current rules for a column to display in UI (with + and delete icons)."""
    try:
        file_rules_dir = os.path.join('rules', file_name.replace('.', '_'))
        rules = load_rules(file_rules_dir, column_name)
        # Format for UI with add/delete indicators
        formatted_rules = [
            {"rule": rule, "add_icon": "+", "delete_icon": f"delete_{i}"}
            for i, rule in enumerate(rules["rules"])
        ]
        return {
            "status": "success",
            "rules": formatted_rules,
            "metadata": {
                "type": rules["type"],
                "dq_dimensions": rules["dq_dimensions"],
                "statistics": rules["statistics"]
            }
        }
    except Exception as e:
        logger.error(f"Error fetching rules: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/invalid-data/{selected_option}/{container_name}/{file_name}")
def generate_invalid_data_queries(selected_option: str, container_name: str, file_name: str):
    try:
        file_base_name = file_name.split('.')[0].lower()
        rules_dir = os.path.join("rules", file_base_name)
        table_name = f"data_{file_base_name.replace('-', '_')}"
        sandbox_engine = create_engine("postgresql://postgres:Lakshmeesh@localhost:5432/DQM-sandbox")
        
        tracker = QueryTracker(output_dir=f"query_logs/{file_base_name}")
        query_generator = SQLQueryGenerator(sandbox_engine, tracker)
        
        results = {'successful_queries': {}, 'failed_queries': {}, 'execution_logs': [], 'cleanup_status': False}
        
        with sandbox_engine.connect() as conn:
            sample_data = {}
            for col in inspect(sandbox_engine).get_columns(table_name):
                result = conn.execute(text(f"SELECT {col['name']} FROM {table_name} LIMIT 10"))
                sample_data[col['name']] = [row[0] for row in result]
        
        total_rules = len([f for f in os.listdir(rules_dir) if f.endswith("_rules.json")])
        if total_rules == 0:
            tracker.logger.warning("No rules files found to process")
            return JSONResponse({"status": "warning", "message": "No rules files found to process", "results": results})
        
        all_queries_successful = True
        for rules_file in os.listdir(rules_dir):
            if not rules_file.endswith("_rules.json"):
                continue
            col_name = rules_file.replace("_rules.json", "")
            try:
                with open(os.path.join(rules_dir, rules_file), 'r') as f:
                    rules = json.load(f)
                query = query_generator.generate_and_test_query(
                    table_name=table_name,
                    column_name=col_name,
                    rules=rules,
                    sample_data=sample_data.get(col_name, [])
                )
                results['successful_queries'][col_name] = query
            except Exception as e:
                all_queries_successful = False
                results['failed_queries'][col_name] = str(e)
                tracker.logger.error(f"Failed to generate/test query for {col_name}: {str(e)}")
        
        if all_queries_successful and len(results['successful_queries']) == total_rules:
            results['cleanup_status'] = query_generator.drop_sandbox_table(table_name)
            tracker.logger.info(f"Cleanup: {results['cleanup_status']}")
        
        log_path = tracker.output_dir / "query_generation.log"
        if log_path.exists():
            with open(log_path, 'r') as f:
                results['execution_logs'] = f.readlines()
        
        return JSONResponse({"status": "success", "results": results})
    
    except Exception as e:
        detailed_error = f"Query generation error: {str(e)}"
        logger.error(detailed_error)
        raise HTTPException(500, detail=detailed_error)

@app.post("/execute-queries/{container_name}/{file_name}")
def execute_stored_queries(container_name: str, file_name: str):
    """Execute pre-tested queries and store invalid records in DQM."""
    try:
        file_base_name = file_name.split('.')[0].lower()
        table_name = f"data_{file_base_name.replace('-', '_')}"
        tracker = QueryTracker(output_dir=f"query_logs/{file_base_name}")
        
        # Use a dummy sandbox engine since we only need main_engine here
        query_generator = SQLQueryGenerator(create_engine("postgresql://postgres:Lakshmeesh@localhost:5432/DQM-sandbox"), tracker)
        execution_results = query_generator.store_invalid_records(table_name)
        
        results = {
            'successful_executions': execution_results['successful_executions'],
            'failed_executions': execution_results['failed_executions'],
            'execution_logs': []
        }
        
        log_path = tracker.output_dir / "query_generation.log"
        if log_path.exists():
            with open(log_path, 'r') as f:
                results['execution_logs'] = f.readlines()
        
        return JSONResponse({"status": "success", "results": results})
    
    except Exception as e:
        detailed_error = f"Query execution error for {file_name}: {str(e)}"
        logger.error(detailed_error)
        raise HTTPException(500, detail=detailed_error)
    
@app.post("/validate-data/{container_name}/{file_name}")
async def validate_data(container_name: str, file_name: str):
    """Read, pivot invalid records, correct them with LLM, and return refactored results."""
    try:
        file_base_name = file_name.split('.')[0].lower()
        table_name = f"data_{file_base_name.replace('-', '_')}"
        tracker = QueryTracker(output_dir=f"query_logs/{file_base_name}")
        
        async with DataValidator(table_name, container_name, file_name) as validator:
            start_time = time.time()
            pivoted_data = validator.read_and_pivot_invalid_records()
            corrected_data = await validator.correct_invalid_records(pivoted_data)
            
            results = {
                "pivoted_data": pivoted_data,
                "successful_validation": corrected_data,  # Now has corrected/uncorrectable/nochange with summary
                "execution_logs": [],
                "execution_time": f"{time.time() - start_time:.2f} seconds"
            }
            
            log_path = tracker.output_dir / "query_generation.log"
            if log_path.exists():
                with open(log_path, 'r') as f:
                    results['execution_logs'] = f.readlines()
            
            return JSONResponse({"status": "success", "results": results})
    
    except Exception as e:
        detailed_error = f"Data validation error for {file_name}: {str(e)}"
        logger.error(detailed_error)
        raise HTTPException(500, detail=detailed_error)

if __name__ == "__main__":
    import nest_asyncio
    import uvicorn
    logging.basicConfig(level=logging.DEBUG)
    nest_asyncio.apply()  # Allow running async in Jupyter/IPython if needed
    uvicorn.run(app, host="0.0.0.0", port=8000)