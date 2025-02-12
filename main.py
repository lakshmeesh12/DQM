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
from sse_starlette.sse import EventSourceResponse
from file_manager import (
    list_s3_buckets, list_s3_files, read_s3_file,
    list_azure_containers, list_azure_files, read_azure_file
)
from llm import column_name_gen, detect_headers, generate_dq_rules
# from data_process import DataStreamProcessor

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

@app.post("/storage/{selected_option}/{container_name}/{file_name}")
async def process_file(
    selected_option: str,
    container_name: str,
    file_name: str,
    file: Optional[UploadFile] = File(None)
):
    """Process selected file and analyze its headers"""
    logger.debug(f"Processing file from {selected_option}/{container_name}/{file_name}")
    try:
        file_content = None
        
        # Get file content based on storage type
        if selected_option == "aws":
            file_content = read_s3_file(container_name, file_name)
        elif selected_option == "azure":
            file_content = read_azure_file(container_name, file_name)
        elif selected_option == "local" and file:
            file_content = await file.read()
            file_content = file_content.decode("utf-8")
        else:
            raise HTTPException(
                status_code=400,
                detail="Invalid storage option or missing file"
            )

        if not file_content:
            raise HTTPException(
                status_code=500,
                detail="No file content was retrieved"
            )

        # Enhanced header detection
        has_headers = detect_headers(file_content)
        logger.debug(f"File has headers: {has_headers}")

        if has_headers:
            df = pd.read_csv(io.StringIO(file_content))
            column_info = {str(i): col for i, col in enumerate(df.columns)}
            generated = False
            logger.debug("Using existing headers")
        else:
            logger.debug("No headers detected, generating using LLM")
            column_info = column_name_gen(file_content)
            generated = True

        # Generate DQ rules
        dq_rules = generate_dq_rules(file_content, column_info)
        
        # Create rules directory if it doesn't exist
        os.makedirs('rules', exist_ok=True)
        
        # Store rules for each column
        for column_name, rules in dq_rules.items():
            rule_file_path = os.path.join('rules', f'{column_name}_rules.json')
            with open(rule_file_path, 'w') as f:
                json.dump(rules, f, indent=2)

        return JSONResponse(content={
            "status": "success",
            "has_headers": has_headers,
            "headers_generated": generated,
            "column_info": column_info,
            "dq_rules": dq_rules
        })

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

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
            file_content = read_s3_file(container_name, file_name)
        elif selected_option == "azure":
            file_content = read_azure_file(container_name, file_name)
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

        # Initialize DataStreamProcessor without chunk_size parameter
        processor = DataStreamProcessor()
        corrected_data, modifications = processor.process_data(df, selected_columns)
        
        return {
            "status": "success",
            "has_headers": has_headers,
            "column_mapping": column_mapping,
            "corrected_data": corrected_data.to_dict(orient='records'),
            "modifications": modifications
        }

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)