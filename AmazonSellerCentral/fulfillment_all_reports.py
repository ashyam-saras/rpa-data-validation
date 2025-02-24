import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
from datetime import datetime
import requests
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_result
from auth import login_and_get_cookie
from io import StringIO
import pandas as pd
from helper.utils import save_content_to_file, parse_args, upload_to_gcs, reset_cookie
from helper.logging import logger
import yaml

COOKIE_STORAGE_PATH = Path(__file__).parent / "auth_state.json"

CONFIG_FILE_PATH = Path(__file__).parent / "report_config" / "fulfillment_all_reports_config.yaml"
with open(CONFIG_FILE_PATH, "r") as file:
    config = yaml.safe_load(file)

MARKET_PLACE_CONFIG_FILE_PATH = Path(__file__).parent / "report_config" / "market_place_config.yaml"
with open(MARKET_PLACE_CONFIG_FILE_PATH, "r") as file:
    market_place_config = yaml.safe_load(file)
marketplace_config = None

BASE_URL = None


def load_report_from_yaml(report_name: str, market_place: str, start_date: str = None, end_date: str = None) -> dict:
    """
    Load configuration details from a YAML file based on the report name and replace start_date and end_date if provided.

    Args:
        report_name: Name of the report to load configuration for
        start_date: Start date to replace in the configuration (optional)
        end_date: End date to replace in the configuration (optional)

    Returns:
        dict: Configuration details for the specified report
    """
    global BASE_URL
    global marketplace_config
    marketplace_config = market_place_config.get("marketplace_config", {}).get(market_place)
    BASE_URL = f"https://sellercentral.amazon.{marketplace_config["fulfillment_url_domain"]}/reportcentral/api/v1"

    report_config = config["fulfillment_reports_config"].get(report_name, {})

    if "reportStartDate" in report_config.get("params", {}) and "reportEndDate" in report_config.get("params", {}):
        if start_date:
            report_config["params"]["reportStartDate"] = start_date
        if end_date:
            report_config["params"]["reportEndDate"] = end_date
    else:
        today = datetime.now().strftime("%Y/%m/%d")
        report_config["params"]["reportStartDate"] = today
        report_config["params"]["reportEndDate"] = today

    return report_config


def validate_parameters(report_start_date: str, report_end_date: str):
    """
    Validate the date parameters for the report.

    Args:
        report_start_date: Start date in YYYY/MM/DD format
        report_end_date: End date in YYYY/MM/DD format

    Raises:
        ValueError: If dates are in invalid format or if end_date is earlier than start_date
    """

    try:
        logger.info("Validating Parameters")

        start_date = datetime.strptime(report_start_date, "%Y/%m/%d")
        end_date = datetime.strptime(report_end_date, "%Y/%m/%d")
        if end_date < start_date:
            raise ValueError("End date cannot be earlier than start date")
    except ValueError as e:
        logger.error(f"Error Validating date: {str(e)}")
        raise e


def request_report(
    cookie: dict,
    params: dict,
    headers: dict,
) -> str:
    """
    Request an report from Amazon Seller Central.

    Args:
        cookie: Configured session cookie dict
        params: Parameters for the report request
        headers: Headers for the request

    Returns:
        tuple: A tuple containing (report_reference_id, report_status)
            - report_reference_id: Unique identifier for the report request
            - report_status: Current status of the report ('Done', 'InQueue', etc.)

    Raises:
        requests.exceptions.RequestException: If the request to Amazon fails
    """
    try:
        logger.info("Requesting report...")

        url = BASE_URL + "/submitDownloadReport"

        response = requests.post(
            url=url,
            params=params,
            cookies=cookie,
            headers=headers,
        )

        logger.info(f" Response Status: {response.status_code}")
        response.raise_for_status()

        if response.status_code == 200:
            json_data = response.json()

            report_reference_id = json_data.get("reportReferenceId")
            report_status = json_data.get("reportStatus")

            return report_reference_id, report_status
        else:
            logger.error(f"Failed to request report. Status code: {response.status_code}")
            return None, None

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        return None, None


@retry(
    stop=stop_after_attempt(15),
    wait=wait_fixed(30),
    retry=retry_if_result(lambda result: result in ["InQueue", "InProgress"]),
)
def check_download_status(cookie: dict, report_reference_id: str):
    """
    Check the status of a report download request.

    Args:
        cookie: Configured session cookie dict
        report_reference_id: Reference ID of the report request

    Returns:
        str: Status of the report ('Done', 'InQueue', 'InProgress', etc.)
        None: If the request to Amazon fails
    """
    try:
        status_url = BASE_URL + "/getDownloadReportStatus"
        response = requests.get(url=status_url, params=[("referenceIds", report_reference_id)], cookies=cookie)
        response.raise_for_status()
        json_data = response.json()
        status = json_data[0] if json_data else None

        logger.info(f"Executed Check Download status function. Status: {status}")

        return status

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        return None


def download_report_data(cookie: dict, report_reference_id: str, file_format: str):
    """
    Download a ready report from Amazon Seller Central.

    Args:
        cookie: Configured session cookie dict
        report_reference_id: Reference ID of the report to download
        file_format: Format of the report file (default: "TSV")

    Returns:
        tuple: A tuple containing (status_code, report_content)
        None: If the download fails
    """

    logger.info("Downloading report...")
    try:
        download_url = BASE_URL + "/downloadFile"
        response = requests.get(
            url=download_url,
            params=[("referenceId", report_reference_id), ("fileFormat", file_format)],
            cookies=cookie,
        )
        response.raise_for_status()
        logger.info(f"{file_format} data saved successfully for report with reference id:{report_reference_id} ")

    except Exception as e:
        logger.error(f"Failed to save data. Status code: {response.status_code}")
        return response.status_code, None

    report_content = response.content if isinstance(response.content, bytes) else response.text.encode("utf-8")
    return response.status_code, report_content


def convert_tsv_to_csv(tsv_data: bytes) -> None:
    """
    Convert TSV data to CSV format and save to file.

    Args:
        tsv_data: String containing TSV formatted data

    Raises:
        Exception: If there's an error during conversion or file saving
    """

    try:
        logger.info("Started converting TSV data to CSV file")
        tsv_data = tsv_data.decode("utf-8")
        # Read TSV data
        string_buffer = StringIO(tsv_data)
        df = pd.read_csv(string_buffer, sep="\t", encoding="utf-8")

        # Convert DataFrame to CSV string
        csv_data = df.to_csv(index=False, encoding="utf-8").encode("utf-8")

        return csv_data

    except Exception as e:
        logger.error(f"Error converting file: {str(e)}")
        raise e


def download_filfillments_report(
    report_start_date: str,
    report_end_date: str,
    reportFileFormat: str,
    params: dict,
    folder_name: str,
    file_prefix: str,
    cookie: dict,
    headers: dict,
    client: str = "nexusbrand",
    brandname: str = "ExplodingKittens",
    bucket_name: str = "rpa_validation_bucket",
):
    """
    Download report from Amazon Seller Central and upload to Google Cloud Storage.

    Args:
        report_start_date: Start date in YYYY/MM/DD format
        report_end_date: End date in YYYY/MM/DD format
        reportFileFormat: Format of the report file
        params: Parameters for the report request
        folder_name: Folder name to save the report
        file_prefix: Prefix for the output file
        client: Client name for GCS path organization (default: "nexusbrand")
        brandname: Brand name for filename and GCS path (default: "ExplodingKittens")
        bucket_name: Google Cloud Storage bucket name (default: "rpa_validation_bucket")

    Raises:
        ValueError: If date parameters are invalid
        Exception: If any error occurs during download or upload process
    """

    try:

        validate_parameters(report_start_date, report_end_date)

        report_reference_id, report_status = request_report(cookie=cookie, params=params, headers=headers)

        logger.info(f"Report Reference ID: {report_reference_id}    Report Status: {report_status}")

        if report_status != "Done":
            download_request_status = check_download_status(cookie=cookie, report_reference_id=report_reference_id)

            if download_request_status != "Done":
                logger.error("Maximum retry reached. Report can not be downloaded")
                return

        status_code, data = download_report_data(
            cookie=cookie, report_reference_id=report_reference_id, file_format=reportFileFormat
        )

        if status_code == 200 and data != None:

            start_date_formatted = datetime.strptime(report_start_date, "%Y/%m/%d").strftime("%Y%m%d")
            end_date_formatted = datetime.strptime(report_end_date, "%Y/%m/%d").strftime("%Y%m%d")

            output_file = f"{file_prefix}_{brandname}_{start_date_formatted}_{end_date_formatted}.csv"
            if reportFileFormat == "TSV":
                data = convert_tsv_to_csv(tsv_data=data)

            # Save using the utility function
            file_path = save_content_to_file(content=data, folder_name=folder_name, file_name=output_file)

            # Extract year and month from report_end_date
            end_date = datetime.strptime(report_end_date, "%Y/%m/%d")
            destination_blob_name = f"UIReport/AmazonSellingPartner/{client}/{brandname}/{file_prefix}/year={end_date.strftime('%Y')}/month={end_date.strftime('%m')}/{output_file}"
            upload_to_gcs(
                local_file_name=output_file,
                local_folder_name=folder_name,
                bucket_name=bucket_name,
                destination_blob_name=destination_blob_name,
            )

    except Exception as e:
        logger.error("Some Error ocurred while downloading")
        raise e


if __name__ == "__main__":
    args = parse_args(
        description="Download Amazon Fulfillment reports for a date range",
        date_format="YYYY/MM/DD",
        optional_args=True,
        amazon_fulfillment=True,
    )

    report_list = args.report_list.split(",")

    cookie = {}
    headers = {}

    while len(cookie) == 0 or len(headers) == 0:
        reset_cookie(cookie_storage_path=COOKIE_STORAGE_PATH)

        cookie, headers = login_and_get_cookie(
            amazon_fulfillment=True,
            market_place=args.market_place,
            username=args.user_name,
            password=args.password,
            otp_secret=args.otp_secret,
            account=args.account,
        )

    for report_name in report_list:

        logger.info(f"GENERATING REPORT FOR {report_name}")
        report_config = load_report_from_yaml(
            report_name=report_name, start_date=args.start_date, end_date=args.end_date, market_place=args.market_place
        )

        params = report_config.get("params")
        folder_name = report_config.get("folder_name")
        file_prefix = report_config.get("file_prefix")
        reportFileFormat = params.get("reportFileFormat")

        download_filfillments_report(
            report_start_date=args.start_date,
            report_end_date=args.end_date,
            params=params,
            reportFileFormat=reportFileFormat,
            file_prefix=file_prefix,
            folder_name=folder_name,
            client=args.client,
            brandname=args.brandname,
            bucket_name=args.bucket_name,
            cookie=cookie,
            headers=headers,
        )
