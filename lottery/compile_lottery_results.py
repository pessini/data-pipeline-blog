import duckdb
import json
import os
from datetime import datetime
from prefect import task, flow, get_run_logger
from prefect_aws.s3 import S3Bucket
from prefect.cache_policies import NO_CACHE
from modules.utils import get_boto3_client_from_prefect_block

@task(retries=2, cache_policy=NO_CACHE)
def get_unprocessed_files(s3_client, bucket_name, folder_prefix="raw-results/"):
    """
    Get all files that have processed=false tag
    """
    logger = get_run_logger()
    unprocessed_files = []
    
    # Use paginator in case more files need processing in the future
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=folder_prefix):
        if 'Contents' not in page:
            continue
        
        for obj in page['Contents']:
            key = obj['Key']
            
            # Get tags for this object
            try:
                response = s3_client.get_object_tagging(Bucket=bucket_name, Key=key)
                tags = response.get('TagSet', [])
                
                # Check if object has processed=false tag
                if any(tag['Key'] == 'processed' and tag['Value'] == 'false' for tag in tags):
                    unprocessed_files.append(key)
            except Exception as e:
                logger.error(f"Error getting tags for {key}: {e}")
    
    return unprocessed_files

@task(retries=2, cache_policy=NO_CACHE)
def mark_file_as_processed(s3_client, bucket_name, object_key):
    """
    Mark a file as processed by changing the processed tag to processed=true.
    """
    logger = get_run_logger()
    try:
        # Get existing tags first
        response = s3_client.get_object_tagging(Bucket=bucket_name, Key=object_key)
        tags = response.get('TagSet', [])
        
        # Update or add the processed=true tag
        tag_updated = False
        for tag in tags:
            if tag['Key'] == 'processed':
                tag['Value'] = 'true'
                tag_updated = True
                break
        
        if not tag_updated:
            # Add the processed=true tag if it doesn't exist
            tags.append({'Key': 'processed', 'Value': 'true'})
        
        # Put updated tags back
        s3_client.put_object_tagging(
            Bucket=bucket_name,
            Key=object_key,
            Tagging={'TagSet': tags}
        )
        logger.info(f"Updated 'processed' tag to 'true' for {object_key}")
    except Exception as e:
        logger.error(f"Error updating tags for {object_key}: {e}")

@task()
def fetch_json_from_minio(s3_block: S3Bucket, file_path: str) -> dict:
    """
    Fetch a JSON file from MinIO for a specific game and draw number.
    """
    logger = get_run_logger()

    try:
        logger.info(f"Fetching file from MinIO: {file_path}")
        content = s3_block.read_path(file_path)
        return json.loads(content)
    except Exception as e:
        logger.error(f"Error fetching file from MinIO: {file_path}. Error: {e}")
        raise

@task(retries=2)
def download_duckdb_from_minio_if_exists(s3_block: S3Bucket, local_path: str):
    logger = get_run_logger()
    file_name = os.path.basename(local_path)
    try:
        logger.info(f"Checking if DuckDB exists in MinIO: {file_name}")
        content = s3_block.read_path(file_name)
        with open(local_path, "wb") as f:
            f.write(content)
        logger.info("Downloaded existing DuckDB from MinIO.")
    except Exception as e:
        logger.warning(f"DuckDB file not found in MinIO (will create new): {e}")

@task()
def create_duckdb_table(db_path: str):
    """
    Create the DuckDB table if it does not exist.
    """
    logger = get_run_logger()
    conn = duckdb.connect(db_path)
    
    try:
        logger.info("Creating DuckDB table if it does not exist...")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS lottery_results (
                game_name TEXT,
                draw_number INTEGER,
                draw_date DATE,
                file_path TEXT,
                winning_numbers JSON,
                prize_tiers JSON,
                PRIMARY KEY (game_name, draw_number)
            )
        """)
        logger.info("DuckDB table creation completed.")
    except Exception as e:
        logger.error(f"Error creating DuckDB table. Error: {e}")
        raise
    finally:
        conn.close()
        
@task()
def save_to_duckdb(data: dict, game: str, db_path: str):
    """
    Save lottery results to a DuckDB database.
    """
    logger = get_run_logger()
    conn = duckdb.connect(db_path)
    try:
        # metadata
        draw_number = data["numero"]
        raw_draw_date = data["dataApuracao"]
        # Convert draw_date to YYYY-MM-DD format
        draw_date = datetime.strptime(raw_draw_date, "%d/%m/%Y").strftime("%Y-%m-%d")
        # Determine the keys for winning numbers and prize tiers
        winning_numbers_key = "listaDezenas" if "listaDezenas" in data else "dezenasSorteadasOrdemSorteio"
        prize_tiers_key = "listaRateioPremio"
        # Insert the data into the DuckDB table
        conn.execute("""
            INSERT INTO lottery_results (game_name, draw_number, draw_date, file_path, winning_numbers, prize_tiers)
            VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING;
        """, (game, draw_number, draw_date, 
              f"raw-results/{game}/{draw_number}.json",
              json.dumps(data[winning_numbers_key]),  # Store winning_numbers as JSON
              json.dumps(data[prize_tiers_key])       # Store prize_tiers as JSON
        ))

        logger.info(f"Successfully saved draw {draw_number} for game {game} to DuckDB.")
    except Exception as e:
        logger.error(f"Error saving data to DuckDB for game {game}, draw {draw_number}. Error: {e}")
        raise

@task
def upload_duckdb_to_minio(s3_block: S3Bucket, db_path: str):
    """
    Upload the DuckDB database file to the root directory of the MinIO bucket.
    """
    logger = get_run_logger()
    file_name = os.path.basename(db_path)
    
    try:
        logger.info(f"Uploading DuckDB database to MinIO: {file_name}")
        with open(db_path, "rb") as db_file:
            s3_block.write_path(path=file_name, content=db_file.read())
        logger.info(f"Successfully uploaded DuckDB database to MinIO: {file_name}")
    except Exception as e:
        logger.error(f"Error uploading DuckDB database to MinIO. Error: {e}")
        raise

@flow
def compile_lottery_results():
    """
    Flow to compile lottery results from MinIO and save them to DuckDB.
    """
    logger = get_run_logger()
    bucket_name = "lottery"  # Replace with your bucket name
    s3_block = S3Bucket.load("s3-lottery")
    s3_client = get_boto3_client_from_prefect_block(s3_block=s3_block)
    
    # Define the local path for the DuckDB database
    db_path = "lottery_results.duckdb"
    # Try to download the existing DB
    download_duckdb_from_minio_if_exists(s3_block=s3_block, local_path=db_path)
    # Create table if needed
    create_duckdb_table(db_path=db_path)
    
    logger.info("Starting compilation of lottery results...")
    
    unprocessed_files = get_unprocessed_files(s3_client=s3_client, bucket_name=bucket_name)
    if not unprocessed_files:
        logger.info("No unprocessed files found. Exiting.")
        return
    
    # Number of unprocessed files
    logger.info(f"Number of unprocessed files: {len(unprocessed_files)}")
    # logger.info(f"Unprocessed files: {unprocessed_files}")
    
    for file_path in unprocessed_files:
        logger.info(f"Processing file: {file_path}")
        # Extract game name and draw number from the file path
        parts = file_path.split("/")
        game = parts[1]  # Assuming the second part is the game name
        draw_number = int(parts[2].replace(".json", ""))  # Extract draw number
        
        try:
            logger.info(f"Game: {game}, Draw Number: {draw_number}")
            # Fetch the JSON file from MinIO
            file_content = fetch_json_from_minio(s3_block=s3_block, file_path=file_path)
            # Save the data to DuckDB
            save_to_duckdb(data=file_content, game=game, db_path=db_path)
            # Mark the file as processed
            mark_file_as_processed(s3_client, bucket_name, file_path)
        except Exception as e:
            logger.error(f"Error processing game {game}. Error: {e}")
            continue
        
    # Upload the DuckDB database to MinIO
    upload_duckdb_to_minio(s3_block=s3_block, db_path=db_path)
    logger.info("Compilation of lottery results completed.")

if __name__ == "__main__":
    compile_lottery_results()
