import os
import boto3
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from fastapi import HTTPException

# Load environment variables
load_dotenv()

# AWS Credentials
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# Azure Storage Credentials
AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_STORAGE_ACCOUNT_SAS_TOKEN = os.getenv("AZURE_STORAGE_ACCOUNT_SAS_TOKEN")

LOCAL_STORAGE_PATH = os.getenv("LOCAL_STORAGE_PATH", "./uploads")
os.makedirs(LOCAL_STORAGE_PATH, exist_ok=True)

# 🟢 List S3 Buckets
def list_s3_buckets():
    try:
        s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        return [bucket["Name"] for bucket in s3.list_buckets().get("Buckets", [])]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching S3 buckets: {e}")

# 🟢 List Files in S3 Bucket
def list_s3_files(bucket_name: str):
    try:
        s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        response = s3.list_objects_v2(Bucket=bucket_name)
        return [file["Key"] for file in response.get("Contents", [])] if "Contents" in response else []
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching files from {bucket_name}: {e}")

# 🟢 Read S3 File Content
def read_s3_file(bucket_name: str, file_name: str):
    try:
        s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        obj = s3.get_object(Bucket=bucket_name, Key=file_name)
        return obj["Body"].read().decode("utf-8")  # Read file content as string
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading {file_name}: {e}")

# 🟢 List Azure Containers
def list_azure_containers():
    try:
        blob_service_client = BlobServiceClient(
            f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net?{AZURE_STORAGE_ACCOUNT_SAS_TOKEN}"
        )
        return [container.name for container in blob_service_client.list_containers()]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching Azure containers: {e}")

# 🟢 List Files in Azure Container
def list_azure_files(container_name: str):
    try:
        blob_service_client = BlobServiceClient(
            f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net?{AZURE_STORAGE_ACCOUNT_SAS_TOKEN}"
        )
        container_client = blob_service_client.get_container_client(container_name)
        return [blob.name for blob in container_client.list_blobs()]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching files from {container_name}: {e}")

# 🟢 Read Azure File Content
def read_azure_file(container_name: str, file_name: str):
    try:
        blob_service_client = BlobServiceClient(
            f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net?{AZURE_STORAGE_ACCOUNT_SAS_TOKEN}"
        )
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
        return blob_client.download_blob().readall().decode("utf-8")  # Read file content as string
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading {file_name}: {e}")

# 🟢 List Local Files
def list_local_files():
    try:
        return [f for f in os.listdir(LOCAL_STORAGE_PATH) if os.path.isfile(os.path.join(LOCAL_STORAGE_PATH, f))]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching local files: {e}")
