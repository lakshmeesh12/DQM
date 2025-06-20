

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
import uuid
import time
import httpx
from typing import Dict, Any
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
from fastapi.responses import Response
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
 
 
@app.get("/proxy/image")
async def proxy_image(url: str):
    """
    Proxy an image from a remote URL to bypass CORS restrictions.
    Usage: /proxy/image?url=https://example.com/image.svg
    """
    try:
        # Validate URL
        if not url.startswith(('http://', 'https://')):
            raise HTTPException(status_code=400, detail="Invalid URL format")

        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            # Check if the response is an image (content-type starts with 'image/')
            if not response.headers.get('content-type', '').startswith('image/'):
                raise HTTPException(status_code=400, detail="URL does not point to an image")

            # Return the image with CORS headers
            return Response(
                content=response.content,
                media_type=response.headers['content-type'],
                headers={
                    "Access-Control-Allow-Origin": "http://localhost:3000",
                    "Cache-Control": "public, max-age=86400"  # Cache for 24 hours
                }
            )
    except httpx.RequestError as e:
        logger.error(f"Error proxying image from {url}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch image: {str(e)}")
    except Exception as e:
        logger.error(f"Error processing image proxy for {url}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing image: {str(e)}")
 
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
 
 
@app.get("/storage/{selected_option}/{container_name}/files")
async def list_files_in_container(selected_option: str, container_name: str):
    """List all files inside the selected container (AWS, Azure, or Local)"""
    logger.debug(f"Listing files in container: {container_name} for {selected_option}")
    try:
        if selected_option == "aws":
            files = list_s3_files(container_name)
            return {"container": container_name, "files": files}
        elif selected_option == "azure":
            files = list_azure_files(container_name)
            return {"container": container_name, "files": files}
        elif selected_option == "local":
            # List files directly from UPLOAD_DIR (ignore container_name)
            files = os.listdir(UPLOAD_DIR)
            return {"container": "local", "files": files}
        else:
            raise HTTPException(status_code=400, detail="Invalid storage option")
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))    
 
@app.get("/storage/{storage_option}/{container}/files")
async def get_files_in_container(storage_option: str, container: str):
    """Get file details for a container in the selected storage option"""
    try:
        if storage_option == "aws":
            files = list_s3_files(container)  # Define this function to fetch file details from AWS S3
        elif storage_option == "azure":
            files = list_azure_files(container)  # Define this function to fetch file details from Azure Blob
        elif storage_option == "local":
            files = os.listdir(os.path.join(UPLOAD_DIR, container))  # List files in local container directory
        else:
            raise HTTPException(status_code=400, detail="Invalid storage option")
 
        return {"files": files}
    except Exception as e:
        logger.error(f"Error fetching files in container {container}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching files in container {container}")
   
UPLOAD_DIR = "./uploads"
 
from fastapi import UploadFile, File
 
@app.get("/storage/{selected_option}/{container_name}/files/{file_name}")
async def read_file_from_storage(selected_option: str, container_name: str, file_name: str):
    """Read the content of a file from a selected storage option"""
    try:
        if selected_option == "aws":
            file_content = read_s3_file(container_name, file_name)
        elif selected_option == "azure":
            file_content = read_azure_file(container_name, file_name)
        elif selected_option == "local":
            # Read file directly from UPLOAD_DIR (ignore container_name)
            file_path = os.path.join(UPLOAD_DIR, file_name)
            if not os.path.exists(file_path):
                raise HTTPException(status_code=404, detail="File not found")
            with open(file_path, "r") as f:
                file_content = f.read()
        else:
            raise HTTPException(status_code=400, detail="Invalid storage option")
 
        return {
            "storage_provider": selected_option,
            "container": container_name,
            "file": file_name,
            "content": file_content
        }
    except Exception as e:
        logger.error(f"Error reading file {file_name} from {selected_option}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error reading file {file_name}")
   
import os
 
 
if not os.path.exists(UPLOAD_DIR):
    os.makedirs(UPLOAD_DIR, exist_ok=True)

@app.post("/storage/local/upload")
async def upload_local_file(file: UploadFile = File(...)):
    """Upload a file to local storage"""
    try:
        # Generate a unique filename to avoid conflicts
        file_extension = os.path.splitext(file.filename)[1]
        unique_filename = f"{uuid.uuid4()}{file_extension}"
        file_location = os.path.join(UPLOAD_DIR, unique_filename)

        # Write the uploaded file to the storage location
        with open(file_location, "wb") as f:
            content = await file.read()  # Use await for async UploadFile
            f.write(content)

        logger.info(f"File {unique_filename} uploaded successfully to {UPLOAD_DIR}")
        # Return response with filename and additional context for frontend
        return JSONResponse(
            status_code=200,
            content={
                "filename": unique_filename,
                "message": "File uploaded successfully",
                "storage_option": "local",
                "container": "local",  # Explicitly specify container for local storage
                "path": file_location  # Optional: include path for debugging
            }
        )
    except PermissionError as e:
        logger.error(f"Permission error uploading file {file.filename}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Permission denied: {str(e)}")
    except Exception as e:
        logger.error(f"Error uploading file {file.filename}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error uploading file: {str(e)}")
   
 
# New route to list the files in the directory
@app.get("/storage/local/files")
async def list_local_files():
    """List files in local storage"""
    try:
        # List all files in the upload directory
        files = os.listdir(UPLOAD_DIR)
        return {"files": files}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching files: {str(e)}")
 
from fastapi.responses import FileResponse  # Importing FileResponse
 
# New route to serve the files
@app.get("/storage/local/files/{file_name}")
async def get_file(file_name: str):
    """Get a file from local storage"""
    try:
        file_path = os.path.join(UPLOAD_DIR, file_name)
        # Check if the file exists
        if os.path.exists(file_path):
            return FileResponse(file_path)
        else:
            raise HTTPException(status_code=404, detail="File not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error serving file: {str(e)}")
 
 
@app.get("/storage/local/{container}/files")
async def list_local_files(container: str):
    """List files in the specified container in local storage."""
    try:
        # Make sure you're pointing to the correct directory based on the container
        container_path = os.path.join(UPLOAD_DIR, container)
       
        # Check if the container exists
        if not os.path.exists(container_path):
            raise HTTPException(status_code=404, detail="Container not found")
       
        # List all files in the container directory
        files = os.listdir(container_path)
        return {"files": files}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching files: {str(e)}")
 
 
 
@app.post("/storage/{selected_option}/{container_name}/{file_name}")
async def store_data(
    selected_option: str,
    container_name: str,
    file_name: str,
    file: Optional[UploadFile] = File(None)
):
    """
    Process and store data from different storage options with DQ rule generation.
    Handles both cases with and without headers, and existing files for rule generation.
    """
    try:
        # Get file content based on storage type
        file_content = None
        if selected_option == "aws":
            file_content = read_s3_file(container_name, file_name)
        elif selected_option == "azure":
            file_content = read_azure_file(container_name, file_name)
        elif selected_option == "local":
            if file:  # If a new file is uploaded
                file_content = await file.read()
                file_content = file_content.decode("utf-8")
            else:  # If generating rules for an existing file
                file_path = os.path.join(UPLOAD_DIR, file_name)
                if not os.path.exists(file_path):
                    raise HTTPException(status_code=404, detail="File not found in local storage")
                with open(file_path, "r") as f:
                    file_content = f.read()
        else:
            raise HTTPException(status_code=400, detail="Invalid storage option")

        if not file_content:
            raise HTTPException(status_code=500, detail="No file content was retrieved")

        # Detect headers
        has_header = detect_headers(file_content)
        
        # Handle case when headers don't exist
        if not has_header:
            # First, parse the CSV into a DataFrame without headers
            temp_df = pd.read_csv(io.StringIO(file_content), header=None)
            
            # Generate column names
            generated_columns = column_name_gen(file_content)
            
            # Get ordered column names based on the actual number of columns in the DataFrame
            num_columns = len(temp_df.columns)
            
            # Log column information for debugging
            logger.info(f"Number of columns in CSV: {num_columns}")
            logger.info(f"Generated column names: {generated_columns}")
            
            # Make sure we have the right number of column names
            if len(generated_columns) != num_columns:
                logger.warning(f"Column count mismatch: CSV has {num_columns} columns but generated {len(generated_columns)} names")
                
                # Adjust generated columns to match DataFrame columns
                if len(generated_columns) < num_columns:
                    # Add generic names for extra columns
                    for i in range(len(generated_columns), num_columns):
                        generated_columns[str(i)] = f"Column_{i+1}"
                else:
                    # Remove excess column names
                    generated_columns = {str(i): generated_columns[str(i)] for i in range(num_columns)}
            
            # Get column names in correct order
            column_names = [generated_columns[str(i)] for i in range(num_columns)]
            
            # Create a new DataFrame with the proper column names
            df = temp_df.copy()
            df.columns = column_names
            
            # Log successful renaming
            logger.info(f"Successfully renamed columns: {df.columns.tolist()}")
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

        # Log column mapping for debugging
        logger.info(f"Column mapping: {column_mapping}")
        
        # Check if all columns in df are in column_mapping
        missing_columns = [col for col in df.columns if col not in column_mapping]
        if missing_columns:
            logger.warning(f"Some columns are missing from mapping: {missing_columns}")
            # Add missing columns to mapping
            for col in missing_columns:
                sanitized_col = table_manager._sanitize_column_name(col)
                column_mapping[col] = sanitized_col

        # Update DataFrame with mapped column names
        mapped_df = df.copy()
        mapped_df.columns = [column_mapping[col] for col in df.columns]
        
        # Clean the DataFrame and process for further operations
        mapped_df = mapped_df.dropna(how='all').reset_index(drop=True)

        # Initialize dq_rules and converted_dq_rules to empty dictionaries
        dq_rules = {}
        converted_dq_rules = {}

        # Detect and generate lookup tables using mapped column names
        try:
            current_df_string = mapped_df.to_csv(index=False)
            lookup_columns = detect_lookup_columns(current_df_string)
            initial_lookup_tables = generate_lookup_tables(current_df_string, lookup_columns) if lookup_columns else None
            
            # Add validation step
            if initial_lookup_tables:
                # Log the original lookup tables to help with debugging
                logger.info(f"Original lookup tables before validation: {initial_lookup_tables}")
                
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
        for col in mapped_df.columns:
            non_null_count = mapped_df[col].notna().sum()
            total_count = len(mapped_df)
            unique_count = mapped_df[col].nunique()
            
            column_statistics[col] = {
                "completeness": (non_null_count / total_count) * 100 if total_count else 0,
                "unique_count": unique_count,
                "uniqueness_ratio": (unique_count / total_count) * 100 if total_count else 0,
                "non_empty_ratio": (mapped_df[col].count() / total_count) * 100 if total_count else 0
            }

        # Store rules in JSON files
        base_rules_dir = os.path.abspath("rules")
        file_base_name = file_name.split('.')[0].lower()
        file_rules_dir = os.path.join(base_rules_dir, file_base_name)
        os.makedirs(file_rules_dir, exist_ok=True)
        
        stored_rules_paths = {}
        for col_name, rules in converted_dq_rules.items():
            sanitized_name = table_manager._sanitize_column_name(col_name)
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
                "row_count": len(mapped_df),
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
        
        # Add traceback to log for detailed debugging
        import traceback
        logger.error(f"Detailed traceback: {traceback.format_exc()}")
        
        raise HTTPException(status_code=500, detail=error_detail)
    
@app.get("/rules/{storage_option}/{container}/{file_name}")
async def fetch_rules_from_directory(storage_option: str, container: str, file_name: str):
    """
    Fetch rules directly from the rules directory for any storage option (AWS, Azure, Local).
    Returns the full rules, details, and statistics for the specified file.
    """
    try:
        # Extract base filename without extension (e.g., "header" from "header.csv" or UUID)
        base_file_name = file_name.split('.')[0].lower()
        file_rules_dir = os.path.join('rules', base_file_name)

        if not os.path.exists(file_rules_dir):
            raise HTTPException(status_code=404, detail="Rules directory not found for this file")

        rules = {}
        statistics = {}
        sanitized_columns = []

        # Read rules and statistics for each column from JSON files in the directory
        for item in os.listdir(file_rules_dir):
            if item.endswith('_rules.json'):
                column_name = item.replace('_rules.json', '')
                sanitized_column = DynamicTableManager()._sanitize_column_name(column_name)
                sanitized_columns.append(sanitized_column)
                rules_file = os.path.join(file_rules_dir, item)
                with open(rules_file, 'r') as f:
                    rules[sanitized_column] = json.load(f)

        # Read metadata for details and statistics (from metadata.json)
        metadata_file = os.path.join(file_rules_dir, "metadata.json")
        if os.path.exists(metadata_file):
            with open(metadata_file, 'r') as f:
                metadata = json.load(f)
                details = {
                    "has_headers": metadata.get("has_headers", False),
                    "original_columns": metadata.get("original_columns", []),
                    "sanitized_columns": metadata.get("sanitized_columns", []),
                    "row_count": metadata.get("row_count", 0),
                    "processed_at": metadata.get("processed_at", datetime.now().isoformat()),
                    "generated_columns": metadata.get("generated_columns", None)
                }
                # Assume statistics are stored in metadata or derived from rules if needed
                for col in sanitized_columns:
                    statistics[col] = {
                        "completeness": 100.0,  # Default or fetch from rules/statistics if available
                        "unique_count": len(rules.get(col, {}).get("rules", [])) if rules.get(col) else 0,
                        "uniqueness_ratio": 100.0,  # Default or calculate based on data
                        "non_empty_ratio": 100.0  # Default or calculate based on data
                    }
        else:
            details = {
                "has_headers": False,
                "original_columns": sanitized_columns,
                "sanitized_columns": sanitized_columns,
                "row_count": 0,
                "processed_at": datetime.now().isoformat(),
                "generated_columns": None
            }
            for col in sanitized_columns:
                statistics[col] = {
                    "completeness": 100.0,
                    "unique_count": len(rules.get(col, {}).get("rules", [])) if rules.get(col) else 0,
                    "uniqueness_ratio": 100.0,
                    "non_empty_ratio": 100.0
                }

        response_data = {
            "status": "success",
            "message": f"Rules fetched from directory for {file_name}",
            "rules": rules,
            "type": "text",
            "statistics": statistics,
            "dq_dimensions": ["Validity", "Completeness", "Relevance"],
            "details": details,
            "sandbox_details": {
                "database": "DQM-sandbox",
                "table_name": base_file_name,
                "status": "rules fetched from directory"
            }
        }

        return JSONResponse(content=response_data)

    except Exception as e:
        logger.error(f"Error fetching rules from directory for {file_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching rules from directory: {str(e)}")
 
@app.post("/rules/{file_name}/{column_name}/add-rule")
async def add_rule_endpoint(
    rule_update: SingleRuleUpdate,
    file_name: str,
    column_name: str  
):
    """Add or edit a single rule for a column via UI (+ symbol). User can input a new rule like 'This column should always start with capital letters'."""
    try:
        # Extract base filename without extension (e.g., "header" from "header.csv" or "header_csv")
        base_file_name = file_name.split('.')[0].lower()
        file_rules_dir = os.path.join('rules', base_file_name)
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
    file_name: str,  
    column_name: str,  
    rule_index: int  
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
    file_name: str,  
    column_name: str  
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
 
# Define UploadRequest model
class UploadRequest(BaseModel):
    new_file_name: str


async def generate_and_test_query_async(query_generator: SQLQueryGenerator, table_name: str, col_name: str, rules: Dict, sample_data: list) -> tuple:
    """Helper function to generate and test query asynchronously"""
    try:
        query = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: query_generator.generate_and_test_query(
                table_name=table_name,
                column_name=col_name,
                rules=rules,
                sample_data=sample_data
            )
        )
        return col_name, query, None
    except Exception as e:
        return col_name, None, str(e)

@app.post("/process-and-execute-queries/{selected_option}/{container_name}/{file_name}")
async def process_and_execute_queries(selected_option: str, container_name: str, file_name: str):
    try:
        file_base_name = file_name.split('.')[0].lower()
        rules_dir = os.path.join("rules", file_base_name)
        table_name = f"data_{file_base_name.replace('-', '_')}"
        sandbox_engine = create_engine("postgresql://postgres:Lakshmeesh@localhost:5432/DQM-sandbox")
        tracker = QueryTracker(output_dir=f"query_logs/{file_base_name}")
        query_generator = SQLQueryGenerator(sandbox_engine, tracker)
        
        results = {
            'successful_queries': {},
            'failed_queries': {},
            'successful_executions': {},
            'failed_executions': {},
            'invalid_data': [],
            'execution_logs': [],
            'cleanup_status': False
        }
        
        # Step 1: Generate and test queries in sandbox concurrently
        with sandbox_engine.connect() as conn:
            sample_data = {}
            for col in inspect(sandbox_engine).get_columns(table_name):
                result = conn.execute(text(f"SELECT {col['name']} FROM {table_name} LIMIT 10"))
                sample_data[col['name']] = [row[0] for row in result]
        
        total_rules = len([f for f in os.listdir(rules_dir) if f.endswith("_rules.json")])
        if total_rules == 0:
            tracker.logger.warning("No rules files found to process")
            return JSONResponse({"status": "warning", "message": "No rules files found to process", "results": results})
        
        # Prepare tasks for concurrent query generation
        tasks = []
        for rules_file in os.listdir(rules_dir):
            if not rules_file.endswith("_rules.json"):
                continue
            col_name = rules_file.replace("_rules.json", "")
            with open(os.path.join(rules_dir, rules_file), 'r') as f:
                rules = json.load(f)
            tasks.append(generate_and_test_query_async(
                query_generator,
                table_name,
                col_name,
                rules,
                sample_data.get(col_name, [])
            ))
        
        # Execute all query generation tasks concurrently
        query_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        all_queries_successful = True
        for col_name, query, error in query_results:
            if error:
                all_queries_successful = False
                results['failed_queries'][col_name] = error
                tracker.logger.error(f"Failed to generate/test query for {col_name}: {error}")
            else:
                results['successful_queries'][col_name] = query
        
        # Step 2: If all queries successful, proceed with execution on main database
        if all_queries_successful and len(results['successful_queries']) == total_rules:
            try:
                execution_results = query_generator.store_invalid_records(table_name)
                results['successful_executions'] = execution_results['successful_executions']
                results['failed_executions'] = execution_results['failed_executions']
                
                # Fetch invalid records
                invalid_table_name = f"invalid_records_{table_name}"
                try:
                    with query_generator.main_engine.connect() as conn:
                        result = conn.execute(text(f"SELECT surrogate_key, invalid_column, invalid_value FROM {invalid_table_name}"))
                        results['invalid_data'] = [
                            {
                                "surrogate_key": row["surrogate_key"],
                                "invalid_column": row["invalid_column"],
                                "invalid_value": row["invalid_value"]
                            }
                            for row in result.mappings()
                        ]
                except SQLAlchemyError as e:
                    logger.warning(f"Could not fetch invalid records from {invalid_table_name}: {str(e)}")
                
                # Perform cleanup
                results['cleanup_status'] = query_generator.drop_sandbox_table(table_name)
                tracker.logger.info(f"Cleanup: {results['cleanup_status']}")
                
            except Exception as e:
                logger.error(f"Execution error: {str(e)}")
                results['failed_executions']['general'] = str(e)
        
        # Load execution logs
        log_path = tracker.output_dir / "query_generation.log"
        if log_path.exists():
            with open(log_path, 'r') as f:
                results['execution_logs'] = f.readlines()
        
        return JSONResponse({"status": "success", "results": results})
    
    except Exception as e:
        detailed_error = f"Processing error for {file_name}: {str(e)}"
        logger.error(detailed_error)
        raise HTTPException(500, detail=detailed_error)

@app.post("/validate-data/{container_name}/{file_name}")
async def validate_data(container_name: str, file_name: str, provider: str = "aws"):
    try:
        file_base_name = file_name.split('.')[0].lower()
        table_name = f"data_{file_base_name.replace('-', '_')}"
        tracker = QueryTracker(output_dir=f"query_logs/{file_base_name}")
       
        async with DataValidator(table_name, provider, container_name, file_name) as validator:
            start_time = time.time()
            
            # Start both operations in parallel
            pivoted_data_task = asyncio.create_task(asyncio.to_thread(validator.read_and_pivot_invalid_records))
            
            # Wait for pivot data to complete
            pivoted_data = await pivoted_data_task
            
            # Process and update records
            successful_validation = await validator.correct_invalid_records(pivoted_data)
            
            # Generate corrected dataset asynchronously while returning the response
            corrected_dataset_task = asyncio.create_task(asyncio.to_thread(validator.generate_corrected_dataset))
            
            # Upload the dataset in the background (don't await completion)
            new_file_name = f"corrected_{file_name}"
            asyncio.create_task(validator.upload_corrected_dataset_to_source(new_file_name))
            
            # Wait for corrected dataset to be ready
            corrected_dataset = await corrected_dataset_task
           
            execution_time = time.time() - start_time
            logger.info(f"Total execution time: {execution_time:.2f} seconds")
            
            results = {
                "pivoted_data": pivoted_data,
                "successful_validation": successful_validation,
                "corrected_dataset": corrected_dataset,
                "execution_logs": [],
                "execution_time": f"{execution_time:.2f} seconds"
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

@app.post("/upload-corrected/{provider}/{container_name}/{file_name}")
async def upload_corrected_dataset(provider: str, container_name: str, file_name: str, request: UploadRequest):
    """Upload the corrected dataset back to the original source with .csv appended."""
    try:
        file_base_name = file_name.split('.')[0].lower()
        table_name = f"data_{file_base_name.replace('-', '_')}"
        new_file_name = f"{request.new_file_name}.csv"  # Append .csv automatically
        
        validator = DataValidator(table_name, provider, container_name, file_name)
        validator.upload_corrected_dataset_to_source(new_file_name)
        
        return JSONResponse({
            "status": "success",
            "message": f"Corrected dataset uploaded as {new_file_name} to {provider} {container_name}"
        })
    except Exception as e:
        detailed_error = f"Error uploading corrected dataset for {file_name}: {str(e)}"
        logger.error(detailed_error)
        raise HTTPException(500, detail=detailed_error)
 
if __name__ == "__main__":
    import nest_asyncio
    import uvicorn
    logging.basicConfig(level=logging.DEBUG)
    nest_asyncio.apply() 
    uvicorn.run(app, host="0.0.0.0", port=8000)
