import boto3
from prefect_aws.s3 import S3Bucket
from prefect import get_run_logger

def get_boto3_client_from_prefect_block(s3_block: S3Bucket) -> boto3.client:
    """
    Create a boto3 S3 client using credentials from a Prefect S3Bucket instance.
    """
    logger = get_run_logger()
    credentials = s3_block.credentials.model_dump()

    # logger.info(f"Credentials: {credentials}")
    if not credentials:
        raise ValueError("Credentials are not set in the S3Bucket block.")
    
    # Extract endpoint_url and use_ssl from aws_client_parameters
    client_params = credentials.get("aws_client_parameters", {})
    endpoint_url = client_params.get("endpoint_url")
    use_ssl = client_params.get("use_ssl", True)

    # Use the endpoint_url and use_ssl directly from the S3Bucket block
    return boto3.client(
        "s3",
        aws_access_key_id=credentials["minio_root_user"],
        aws_secret_access_key=credentials["minio_root_password"].get_secret_value(),  # Extract the secret value
        endpoint_url=endpoint_url,  # Use the endpoint_url from the S3Bucket block
        use_ssl=use_ssl,  # Use the use_ssl flag from the S3Bucket block
    )