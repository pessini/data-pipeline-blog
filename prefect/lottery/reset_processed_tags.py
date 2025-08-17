import boto3
from prefect import task, flow, get_run_logger
from prefect_aws.s3 import S3Bucket
from modules.utils import get_boto3_client_from_prefect_block
from prefect.cache_policies import NO_CACHE

@task(retries=2, cache_policy=NO_CACHE)
def get_processed_files(s3_client, bucket_name, folder_prefix="raw-results/"):
    """
    Get all files that have processed=true tag.
    """
    logger = get_run_logger()
    processed_files = []

    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=folder_prefix):
        if 'Contents' not in page:
            continue

        for obj in page['Contents']:
            key = obj['Key']

            try:
                response = s3_client.get_object_tagging(Bucket=bucket_name, Key=key)
                tags = response.get('TagSet', [])

                if any(tag['Key'] == 'processed' and tag['Value'] == 'true' for tag in tags):
                    processed_files.append(key)
            except Exception as e:
                logger.error(f"Error getting tags for {key}: {e}")

    return processed_files

@task(retries=2, cache_policy=NO_CACHE)
def mark_file_as_unprocessed(s3_client, bucket_name, object_key):
    """
    Change the processed tag to processed=false.
    """
    logger = get_run_logger()
    try:
        response = s3_client.get_object_tagging(Bucket=bucket_name, Key=object_key)
        tags = response.get('TagSet', [])

        for tag in tags:
            if tag['Key'] == 'processed':
                tag['Value'] = 'false'
                break

        s3_client.put_object_tagging(
            Bucket=bucket_name,
            Key=object_key,
            Tagging={'TagSet': tags}
        )
        logger.info(f"Updated 'processed' tag to 'false' for {object_key}")
    except Exception as e:
        logger.error(f"Error updating tags for {object_key}: {e}")

@flow
def reset_processed_tags():
    """
    Flow to reset processed=true tags to processed=false.
    """
    logger = get_run_logger()
    bucket_name = "lottery"  # Replace with your bucket name
    s3_block = S3Bucket.load("s3-lottery")
    s3_client = get_boto3_client_from_prefect_block(s3_block=s3_block)

    logger.info("Fetching files with processed=true tag...")
    processed_files = get_processed_files(s3_client=s3_client, bucket_name=bucket_name)

    if not processed_files:
        logger.info("No files with processed=true tag found. Exiting.")
        return

    logger.info(f"Number of files to reset: {len(processed_files)}")

    for file_path in processed_files:
        mark_file_as_unprocessed(s3_client=s3_client, bucket_name=bucket_name, object_key=file_path)

    logger.info("Reset of processed tags completed.")

if __name__ == "__main__":
    reset_processed_tags()