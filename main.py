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
from sse_starlette.sse import EventSourceResponse
from file_manager import (
    list_s3_buckets, list_s3_files, read_s3_file,
    list_azure_containers, list_azure_files, read_azure_file
)
from llm import column_name_gen, detect_headers, generate_dq_rules, detect_lookup_columns, generate_lookup_tables
# from data_process import DataStreamProcessor
from llm import validate_lookup_tables
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

@app.post("/invalid-data/{selected_option}/{container_name}/{file_name}")
async def generate_invalid_data_queries(
    selected_option: str,
    container_name: str,
    file_name: str
):
    try:
        # Setup
        file_base_name = file_name.split('.')[0].lower()
        rules_dir = os.path.join("rules", file_base_name)
        table_name = f"data_{file_base_name.replace('-', '_')}"
        sandbox_engine = create_engine("postgresql://postgres:Lakshmeesh@localhost:5432/DQM-sandbox")
        
        # Initialize enhanced components
        tracker = QueryTracker(output_dir=f"query_logs/{file_base_name}")
        query_generator = SQLQueryGenerator(sandbox_engine, tracker)
        
        # Process rules and generate queries
        results = {
            'successful_queries': {},
            'failed_queries': {},
            'execution_logs': [],
            'cleanup_status': False
        }
        
        # Get sample data
        with sandbox_engine.connect() as conn:
            sample_data = {}
            for col in inspect(sandbox_engine).get_columns(table_name):
                result = conn.execute(text(f"SELECT {col['name']} FROM {table_name} LIMIT 10"))
                sample_data[col['name']] = [row[0] for row in result]
        
        total_rules = len([f for f in os.listdir(rules_dir) if f.endswith("_rules.json")])
        if total_rules == 0:
            tracker.logger.warning("No rules files found to process")
            return JSONResponse({
                "status": "warning",
                "message": "No rules files found to process",
                "results": results
            })
            
        all_queries_successful = True  # Flag to track if all queries succeed
        
        # Process each rules file
        for rules_file in os.listdir(rules_dir):
            if not rules_file.endswith("_rules.json"):
                continue
                
            col_name = rules_file.replace("_rules.json", "")
            
            try:
                with open(os.path.join(rules_dir, rules_file), 'r') as f:
                    rules = json.load(f)
                
                # This will only return if query generation AND testing succeeds
                query = query_generator.generate_and_test_query(
                    table_name=table_name,
                    column_name=col_name,
                    rules=rules,
                    sample_data=sample_data.get(col_name, [])
                )
                
                results['successful_queries'][col_name] = query
                
            except Exception as e:
                all_queries_successful = False  # Mark as failed if any query fails
                results['failed_queries'][col_name] = str(e)
                tracker.logger.error(f"Failed to generate/test query for {col_name}: {str(e)}")
        
        # Only drop the table if all queries were successfully generated AND tested
        if all_queries_successful and len(results['successful_queries']) == total_rules:
            results['cleanup_status'] = query_generator.drop_sandbox_table(table_name)
            if results['cleanup_status']:
                tracker.logger.info(f"Successfully dropped sandbox table {table_name} after all queries passed testing")
            else:
                tracker.logger.warning(f"Failed to drop sandbox table {table_name} after successful testing")
        else:
            tracker.logger.info(f"Keeping sandbox table {table_name} as some queries failed generation/testing")
        
        # Get execution logs
        log_path = tracker.output_dir / "query_generation.log"
        if log_path.exists():
            with open(log_path, 'r') as f:
                results['execution_logs'] = f.readlines()
        
        return JSONResponse({
            "status": "success",
            "results": results
        })
        
    except Exception as e:
        logger.error(f"Query generation error: {str(e)}")
        raise HTTPException(500, detail=str(e))
 
# @app.get("/table-schema/{table_name}")
# async def get_table_schema(table_name: str):
#     try:
#         db_manager = DynamicTableManager()
#         schema = db_manager.get_table_schema(table_name)
#         return JSONResponse(content=schema)
#     except DatabaseError as e:
#         logger.error(f"Database error in get_table_schema: {str(e)}")
#         raise HTTPException(status_code=500, detail=str(e))
#     except Exception as e:
#         logger.error(f"Error in get_table_schema: {str(e)}")
#         raise HTTPException(status_code=500, detail=str(e))

# @app.post("/storage/{selected_option}/{container_name}/{file_name}")
# async def process_file(
#     selected_option: str,
#     container_name: str,
#     file_name: str,
#     file: Optional[UploadFile] = File(None)
# ):
#     """Process selected file and analyze its headers"""
#     logger.debug(f"Processing file from {selected_option}/{container_name}/{file_name}")
#     try:
#         file_content = None
        
#         # Get file content based on storage type
#         if selected_option == "aws":
#             file_content = read_s3_file(container_name, file_name)
#         elif selected_option == "azure":
#             file_content = read_azure_file(container_name, file_name)
#         elif selected_option == "local" and file:
#             file_content = await file.read()
#             file_content = file_content.decode("utf-8")
#         else:
#             raise HTTPException(
#                 status_code=400,
#                 detail="Invalid storage option or missing file"
#             )

#         if not file_content:
#             raise HTTPException(
#                 status_code=500,
#                 detail="No file content was retrieved"
#             )

#         # Enhanced header detection
#         has_headers = detect_headers(file_content)
#         logger.debug(f"File has headers: {has_headers}")

#         if has_headers:
#             df = pd.read_csv(io.StringIO(file_content))
#             column_info = {str(i): col for i, col in enumerate(df.columns)}
#             generated = False
#             logger.debug("Using existing headers")
#         else:
#             logger.debug("No headers detected, generating using LLM")
#             column_info = column_name_gen(file_content)
#             generated = True

#         # Generate DQ rules
#         dq_rules = generate_dq_rules(file_content, column_info)
        
#         # Create rules directory if it doesn't exist
#         os.makedirs('rules', exist_ok=True)
        
#         # Store rules for each column
#         for column_name, rules in dq_rules.items():
#             rule_file_path = os.path.join('rules', f'{column_name}_rules.json')
#             with open(rule_file_path, 'w') as f:
#                 json.dump(rules, f, indent=2)

#         return JSONResponse(content={
#             "status": "success",
#             "has_headers": has_headers,
#             "headers_generated": generated,
#             "column_info": column_info,
#             "dq_rules": dq_rules
#         })

#     except Exception as e:
#         logger.error(f"Error: {str(e)}")
#         raise HTTPException(status_code=500, detail=str(e))

# Replace the validate-data endpoint with this updated version
@app.post("/validate-data/{selected_option}/{container_name}/{file_name}")
async def validate_and_correct_data(
    selected_option: str,
    container_name: str,
    file_name: str,
    file: Optional[UploadFile] = File(None),
    column_selection: str = Form(...)
):
    logger.debug(f"Validating data from {selected_option}/{container_name}/{file_name}")
    
    try:
        column_selection_data = json.loads(column_selection)
        selected_columns = column_selection_data.get('selected_columns', [])
        if not selected_columns or not isinstance(selected_columns, list):
            raise HTTPException(status_code=400, detail="Invalid column_selection format")

        file_content = None
        if selected_option == "aws":
            file_content = await read_s3_file(container_name, file_name)
        elif selected_option == "azure":
            file_content = await read_azure_file(container_name, file_name)
        elif selected_option == "local" and file:
            file_content = await file.read()
            file_content = file_content.decode("utf-8")
        else:
            raise HTTPException(status_code=400, detail="Invalid storage option or missing file")

        if not file_content:
            raise HTTPException(status_code=500, detail="No file content was retrieved")

        has_headers = detect_headers(file_content)
        
        df = pd.read_csv(io.StringIO(file_content))
        if not has_headers:
            column_mapping = column_name_gen(file_content)
            df.columns = [column_mapping[str(i)] for i in range(len(df.columns))]
        else:
            column_mapping = {str(i): col for i, col in enumerate(df.columns)}

        processor = DataStreamProcessor()
        corrected_data, modifications = await processor.process_data(df, selected_columns)
        
        
        # Clean the DataFrame for JSON serialization
        corrected_dict = corrected_data.replace([np.inf, -np.inf], None).where(pd.notnull(corrected_data), None).to_dict(orient='records')
        
        return {
            "status": "success",
            "has_headers": has_headers,
            "column_mapping": column_mapping,
            "corrected_data": corrected_dict,
            "modifications": modifications
        }

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)