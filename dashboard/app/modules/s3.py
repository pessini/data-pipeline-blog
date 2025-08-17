import os
import boto3
import tempfile
import streamlit as st
import ssl
import time
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Global S3 client
_s3_client = None


def _init_s3_client():
    """Initialize S3 client with MinIO configuration"""
    global _s3_client
    
    if _s3_client is not None:
        return _s3_client
        
    try:
        s3_endpoint = os.getenv('S3_ENDPOINT_URL')
        s3_access_key = os.getenv('S3_ACCESS_KEY_ID')
        s3_secret_key = os.getenv('S3_SECRET_ACCESS_KEY')
        s3_region = os.getenv('S3_REGION', 'us-east-1')
        
        # Determine if we should use SSL based on endpoint URL
        use_ssl = s3_endpoint.startswith('https://')
        
        # For Caddy internal TLS, we might need to disable certificate verification
        verify_ssl = False if use_ssl else True
        
        _s3_client = boto3.client(
            's3',
            endpoint_url=s3_endpoint,
            aws_access_key_id=s3_access_key,
            aws_secret_access_key=s3_secret_key,
            region_name=s3_region,
            use_ssl=use_ssl,
            verify=verify_ssl  # Disable verification for Caddy internal TLS
        )
        return _s3_client
        
    except Exception as e:
        st.error(f"Failed to initialize S3 client: {str(e)}")
        return None


def download_duckdb_file():
    """Download DuckDB file from S3"""
    s3_client = _init_s3_client()
    if not s3_client:
        st.error("S3 client not initialized")
        return None
    
    try:
        s3_bucket = os.getenv('S3_BUCKET_NAME')
        duckdb_file = os.getenv('DUCKDB_FILE_PATH')
        
        # Create a temporary file
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.duckdb')
        temp_file_path = temp_file.name
        temp_file.close()
        
        # Download the file from S3
        s3_client.download_file(
            s3_bucket, 
            duckdb_file, 
            temp_file_path
        )
        
        return temp_file_path
        
    except ClientError as e:
        st.error(f"Failed to download DuckDB file from S3: {str(e)}")
        return None
    except Exception as e:
        st.error(f"Unexpected error downloading DuckDB file: {str(e)}")
        return None


def test_s3_connection():
    """Test the S3 connection"""
    s3_client = _init_s3_client()
    if not s3_client:
        return False, "S3 client not initialized"
    
    try:
        s3_bucket = os.getenv('S3_BUCKET_NAME')
        # Try to list objects in the bucket
        response = s3_client.list_objects_v2(
            Bucket=s3_bucket,
            MaxKeys=1
        )
        return True, "Connection successful"
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_msg = e.response['Error']['Message']
        return False, f"S3 Error [{error_code}]: {error_msg}"
    except ssl.SSLError as e:
        return False, f"SSL Error: {str(e)} - Try disabling SSL verification"
    except Exception as e:
        return False, f"Connection failed: {str(e)}"
