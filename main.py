from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from typing import Optional
import logging
import pandas as pd
import io
from file_manager import (
    list_s3_buckets, list_s3_files, read_s3_file,
    list_azure_containers, list_azure_files, read_azure_file
)
from llm import column_name_gen, detect_headers, generate_dq_rules

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
            # If file has headers, read them directly
            df = pd.read_csv(io.StringIO(file_content))
            column_info = {str(i): col for i, col in enumerate(df.columns)}
            generated = False
            logger.debug("Using existing headers")
        else:
            # Only generate headers if none exist
            logger.debug("No headers detected, generating using LLM")
            column_info = column_name_gen(file_content)
            generated = True

        # Generate DQ rules
        dq_rules = generate_dq_rules(file_content, column_info)

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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)