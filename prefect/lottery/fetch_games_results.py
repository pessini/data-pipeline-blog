from prefect import flow, task, get_run_logger
from prefect.variables import Variable
import requests
from typing import Optional
import json
import os
from prefect_aws.s3 import S3Bucket
from datetime import datetime
from modules.utils import get_boto3_client_from_prefect_block
from prefect.cache_policies import NO_CACHE

@task(retries=3)
def fetch_lottery_result(game: str, draw_number: Optional[int] = None) -> dict:
    """
    Fetch lottery results from the Caixa API
    """
    logger = get_run_logger()
    
    base_url = f"https://servicebus2.caixa.gov.br/portaldeloterias/api/{game}"
    url = f"{base_url}/{draw_number}" if draw_number else f"{base_url}"
    
    logger.info(f"Fetching {game} results from: {url}")
    try:
        response = requests.get(url)
        
        # Handle specific HTTP status codes
        if response.status_code == 404:
            logger.warning(f"Invalid game name: {game}. API returned 404. Skipping this game.")
            return None
        elif response.status_code == 500:
            logger.warning(f"Invalid draw number: {draw_number}. API returned 500. Skipping this draw.")
            return None
        elif response.status_code != 200:
            logger.warning(f"Unexpected API response: {response.status_code} - {response.text}. Skipping.")
            return None
        
        # Return the JSON response if status code is 200
        return response.json()
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error while fetching results from API: {e}. Skipping this step.")
        return None

@task(retries=3)
def save_to_minio(data: dict, game: str, draw_number: Optional[int] = None) -> str:
    """
    Save lottery results to Minio using Prefect blocks
    """
    logger = get_run_logger()
    logger.info(f"Saving {game} results to Minio...")    

    s3_block = S3Bucket.load("s3-lottery")
    
    if not s3_block:
        logger.error("S3 bucket block not found.")
        raise ValueError("S3 bucket block not found.")
    # Ensure the data is in JSON format
    if not isinstance(data, dict):
        logger.error("Data is not in JSON format.")
        raise ValueError("Data is not in JSON format.")
    # Ensure the data is not empty
    if not data:
        logger.error("Data is empty.")
        raise ValueError("Data is empty.")
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    draw = draw_number if draw_number else data.get("numero", "latest")
    filename = f"raw-results/{game}/{draw}.json"
    
    # Ensure the filename is safe
    base_dir = "raw-results"
    safe_path = os.path.join(base_dir, game, f"{draw}.json")
    safe_path = os.path.normpath(safe_path)
    if not safe_path.startswith(base_dir):
        logger.error("Invalid path detected")
        raise ValueError("Invalid path detected")
    
    # Check if the file already exists in Minio
    try:
        existing_file = s3_block.read_path(path=filename)
        if existing_file:
            logger.info(f"File already exists: s3://{s3_block.bucket_name}/{filename}. Skipping save.")
            return None  # Return None if the file already exists
    except Exception as e:
        logger.info(f"File does not exist: s3://{s3_block.bucket_name}/{filename}. Proceeding to save.")
    
    # Save data to MinIO
    try:
        json_data = json.dumps(data).encode('utf-8')
        s3_block.write_path(
            path=filename,
            content=json_data
        )
        logger.info(f"Saved results to s3://{s3_block.bucket_name}/{filename}")
        return filename  # Return the filename if the file was successfully saved
    except Exception as e:
        logger.error(f"Error saving file to MinIO: {e}")
        return None  # Return None if the save operation failed

@task(retries=2, cache_policy=NO_CACHE)
def tag_file_as_unprocessed(s3_client, bucket_name: str, object_key: str):
    """
    Add a tag 'processed:false' to the specified file in the S3 bucket.
    """
    logger = get_run_logger()
    try:
        # Add the processed=false tag
        tags = [{'Key': 'processed', 'Value': 'false'}]
        
        # Put the tags on the object
        s3_client.put_object_tagging(
            Bucket=bucket_name,
            Key=object_key,
            Tagging={'TagSet': tags}
        )
        logger.info(f"Added 'processed:false' tag to {object_key}")
    except Exception as e:
        logger.error(f"Error tagging file {object_key} as unprocessed: {e}")
        raise

@flow
def fetch_lottery_results(draw_number: Optional[int] = None):
    """
    Flow to fetch lottery results and save to Minio
    """
    logger = get_run_logger()
    logger.info("Starting lottery results flow...")
    # Get the list of games from Prefect variable
    games = Variable.get("lottery_games", default="lotofacil")
    # Ensure the games variable is not empty
    if not games:
        logger.error("No games found in the lottery_games variable.")
        raise ValueError("No games found in the lottery_games variable.")
    
    logger.info(f"Starting lottery results flow for games: {games}")
    # Initialize the S3 client
    s3_block = S3Bucket.load("s3-lottery")
    s3_client = get_boto3_client_from_prefect_block(s3_block=s3_block)
    bucket_name = s3_block.bucket_name
    
    # Loop through each game in the games list
    for game in games:
        logger.info(f"Processing game: {game}")
        
        # Fetch results
        results = fetch_lottery_result(game=game, draw_number=draw_number)
        
        # Skip saving if results are None
        # This condition checks if the fetch_lottery_result function returned None.
        # This happens when there was an error during the API call (e.g., invalid game name, invalid draw number, or a network issue).
        if results is None:
            logger.warning(f"Skipping save for game: {game}, draw: {draw_number} due to fetch error.")
            continue
        # Check if results are empty
        # This condition checks if the results object is empty (e.g., an empty dictionary {} or an empty list []).
        # This happens when the API call was successful (status code 200), but the response body contains no data.
        if not results:
            logger.warning(f"No results found for game: {game}, draw: {draw_number}. Skipping save.")
            continue
        
        # Save results to Minio
        logger.info(f"Saving results for game: {game}, draw: {draw_number}")
        # Save to Minio
        filename = save_to_minio(data=results, game=game, draw_number=draw_number)
        
        if filename:
            # Add the 'processed:false' tag to the file
            tag_file_as_unprocessed(s3_client=s3_client, bucket_name=bucket_name, object_key=filename)
        
        logger.info(f"Completed processing for Game: {game}. Draw: {draw_number}")
    
    logger.info("Flow completed successfully for all games.")

if __name__ == "__main__":
    fetch_lottery_results()