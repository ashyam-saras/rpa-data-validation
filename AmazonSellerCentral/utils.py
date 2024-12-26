from google.cloud import storage
from google.cloud.storage import Blob
from pathlib import Path
import json
import argparse
from logger import logger

STORAGE_STATE_PATH = Path(__file__).parent / "data"
SERVICE_ACCOUNT_PATH = Path(__file__).parent / "solutionsdw_rpa_data_validation_bot.json"

def save_content_to_file(content: bytes, folder_name: str, file_name: str) -> Path:
    """
    Save content to a file in the specified folder.
    
    Args:
        content: The content to save (in bytes)
        folder_name: Name of the folder under data directory
        file_name: Name of the file to save
        
    Returns:
        Path: Path object pointing to the saved file
        
    Raises:
        Exception: If there's an error saving the file
    """
    try:
        file_path = STORAGE_STATE_PATH / folder_name / file_name
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(file_path, 'wb') as f:
            f.write(content)
            
        logger.info(f"File saved successfully to: {file_path}")
        return file_path
    except Exception as e:
        logger.error(f"Error saving file: {str(e)}")
        raise e

def upload_to_gcs( destination_blob_name: str, local_file_name: str="", local_folder_name: str="", bucket_name: str="rpa_validation_bucket") -> None:
    """
    Upload a file to Google Cloud Storage bucket.
    
    Args:
        local_file_path: Path to the local file to upload
        bucket_name: Name of the GCS bucket
        destination_blob_name: Name to give the file in GCS
    """
    try:

        logger.info("Started Uploading to GCS")

        local_file_path = STORAGE_STATE_PATH / str(Path(local_folder_name)) / str(Path((local_file_name )))

        logger.info(f"Creating Client")

        storage_client = storage.Client.from_service_account_json(str(SERVICE_ACCOUNT_PATH))

        logger.info(f"Getting Bucket")
        bucket = storage_client.bucket(bucket_name)

        logger.info(f"Getting Blob")
        blob = bucket.blob(destination_blob_name)

        logger.info(f"Uploading File")
        blob.upload_from_filename(local_file_path)

        logger.info(f"File {local_file_name} uploaded to {destination_blob_name} in bucket {bucket_name}")
    
    except Exception as e:
        logger.error(f"Error uploading to GCS: {str(e)}")
        raise e

def parse_args(description: str, date_format: str, optional_args: bool = False, amazon_ads: bool = False):
    """
    Parse command line arguments for report date range.
    
    Args:
        description: Description of the script for help text
        date_format: Date format string (e.g., 'YYYY/MM/DD' or 'YYYY-MM-DD')
        optional_args: Whether to include optional arguments for client, brand, and bucket
    
    Returns:
        argparse.Namespace: Parsed command line arguments
    """
    parser = argparse.ArgumentParser(description=description)
    # Required arguments
    parser.add_argument('--start_date', 
                      required=True,
                      help=f'Start date in {date_format} format')
    parser.add_argument('--end_date',
                      required=True,
                      help=f'End date in {date_format} format')
    
    if optional_args:
        # Optional arguments
        parser.add_argument('--client', type=str, default="nexusbrand",
                          help='(Optional) Client name (default: nexusbrand)')
        parser.add_argument('--brandname', type=str, default="ExplodingKittens",
                          help='(Optional) Brand name (default: ExplodingKittens)')
        parser.add_argument('--bucket_name', type=str, default="rpa_validation_bucket",
                          help='(Optional) GCS bucket name (default: rpa_validation_bucket)')
    
    if amazon_ads:
        parser.add_argument('--report_list', type=str, required=True,
                          help="comma separeted names of Amazon Ads Report (can be found in readme)")
    
    return parser.parse_args()