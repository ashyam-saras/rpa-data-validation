import os
from google.cloud import storage
from pathlib import Path
import argparse
from helper.logging import logger

STORAGE_STATE_PATH = Path(__file__).parent.parent / "data"
SERVICE_ACCOUNT_PATH = Path(__file__).parent.parent / "solutionsdw_rpa_data_validation_bot.json"


def save_content_to_file(content: bytes, folder_name: str, file_name: str) -> str:
    """
    Save content to a file, adding double quotes around CSV values if not already present.

    Args:
        content (bytes): The content to save.
        folder_name (str): The folder to save the file in.
        file_name (str): The name of the file.

    Returns:
        str: The path to the saved file.
    """
    # Ensure the folder exists
    try:
        file_path = STORAGE_STATE_PATH / folder_name / file_name
        file_path.parent.mkdir(parents=True, exist_ok=True)

        # Convert bytes content to string, replacing invalid bytes
        content_str = content.decode("utf-8", errors="replace")

        # Add double quotes around CSV values if not already present
        lines = content_str.splitlines()
        quoted_lines = []
        for line in lines:
            values = line.split(",")
            quoted_values = [
                f'"{value}"' if not (value.startswith('"') and value.endswith('"')) else value for value in values
            ]
            quoted_lines.append(",".join(quoted_values))
        quoted_content = "\n".join(quoted_lines)

        # Save the content to a file
        with open(file_path, "w", encoding="utf-8") as file:
            file.write(quoted_content)
    except Exception as e:
        logger.error(f"Error saving file: {str(e)}")
        raise e

    return file_path


def upload_to_gcs(
    destination_blob_name: str,
    local_file_name: str = "",
    local_folder_name: str = "",
    bucket_name: str = "rpa_validation_bucket",
) -> None:
    """
    Upload a file to Google Cloud Storage bucket.

    Args:
        local_file_path: Path to the local file to upload
        bucket_name: Name of the GCS bucket
        destination_blob_name: Name to give the file in GCS
    """
    try:

        logger.info("Started Uploading to GCS")

        local_file_path = STORAGE_STATE_PATH / str(Path(local_folder_name)) / str(Path((local_file_name)))

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


def parse_args(
    description: str,
    date_format: str,
    optional_args: bool = False,
    amazon_ads: bool = False,
    amazon_fulfillment: bool = False,
) -> argparse.Namespace:
    """
    Parse command line arguments for report date range.

    Args:
        description: Description of the script for help text
        date_format: Date format string (e.g., 'YYYY/MM/DD' or 'YYYY-MM-DD')
        optional_args: Whether to include optional arguments for client, brand, and bucket
        amazon_ads: Whether to include optional argument for Amazon Ads report list
    Returns:
        argparse.Namespace: Parsed command line arguments
    """
    parser = argparse.ArgumentParser(description=description)
    # Required arguments
    parser.add_argument("--start_date", required=True, help=f"Start date in {date_format} format")
    parser.add_argument("--end_date", required=True, help=f"End date in {date_format} format")

    parser.add_argument(
        "--user_name",
        type=str,
        required=True,
        help="Client Username to login",
    )
    parser.add_argument(
        "--password",
        type=str,
        required=True,
        help="Client Password to login",
    )
    parser.add_argument(
        "--otp_secret",
        type=str,
        required=True,
        help="Client OTP Secret to login",
    )

    if optional_args:
        # Optional arguments
        parser.add_argument(
            "--client", type=str, default="nexusbrand", help="(Optional) Client name (default: nexusbrand)"
        )
        parser.add_argument(
            "--brandname",
            type=str,
            default="ExplodingKittens",
            help="(Optional) Brand name (default: ExplodingKittens)",
        )
        parser.add_argument(
            "--bucket_name",
            type=str,
            default="rpa_validation_bucket",
            help="(Optional) GCS bucket name (default: rpa_validation_bucket)",
        )
        parser.add_argument(
            "--market_place",
            type=str,
            default="United States",
            help="(Optional) Market Place Name (default: United States)",
        )

    if amazon_ads or amazon_fulfillment:
        parser.add_argument(
            "--report_list",
            type=str,
            required=True,
            help="comma separeted names of Amazon Reports (can be found in readme)",
        )

    return parser.parse_args()


def reset_cookie(cookie_storage_path: str):
    # Remove auth_state.json file to clear cookies
    try:

        print("Cookie storage path", cookie_storage_path)

        if cookie_storage_path.exists():
            print("Removing auth_state.json")
            os.remove(cookie_storage_path)
    except Exception as e:
        logger.error(f"Error removing auth_state.json: {str(e)}")
        raise e
