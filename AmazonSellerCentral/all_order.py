from datetime import datetime
from pathlib import Path
import requests
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_result
from pathlib import Path
from auth import login_and_get_cookie
from io import StringIO
import pandas as pd
from utils import upload_to_gcs, save_content_to_file, parse_args
import argparse
from logger import logger

STORAGE_STATE_PATH = Path(__file__).parent / "data" / "allorders"
BASE_URL = "https://sellercentral.amazon.com/reportcentral/api/v1"


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

def request_allOrder_report(
    cookie: dict,
    report_start_date: str,
    report_end_date: str,
    startDateTimeOffset: int = 0,
    endDateTimeOffset: int = 0
) -> str:
    """
    Request an All Orders report from Amazon Seller Central.

    Args:
        cookie: Configured session cookie dict
        report_start_date: Start date in YYYY/MM/DD format
        report_end_date: End date in YYYY/MM/DD format
        startDateTimeOffset: Offset in hours for start date (default: 0)
        endDateTimeOffset: Offset in hours for end date (default: 0)

    Returns:
        tuple: A tuple containing (report_reference_id, report_status)
            - report_reference_id: Unique identifier for the report request
            - report_status: Current status of the report ('Done', 'InQueue', etc.)

    Raises:
        requests.exceptions.RequestException: If the request to Amazon fails
    """

    logger.info("Requesting AllOrders report...")

    url = BASE_URL + "/submitDownloadReport"

    response = requests.post(
        url=url,
        params=[
            ("reportFileFormat", "TSV"),
            ("reportStartDate", report_start_date),
            ("reportEndDate", report_end_date),
            ("startDateTimeOffset", startDateTimeOffset),
            ("endDateTimeOffset", endDateTimeOffset),
            ("xdaysBeforeUntilToday", -1),
            ("reportFRPId", "2400"),
            ("disableTimezone", "true"),
        ],
        cookies=cookie,
    )

    json_data = response.json()

    report_reference_id = json_data.get("reportReferenceId")
    report_status = json_data.get("reportStatus")

    return report_reference_id, report_status


@retry(
    stop=stop_after_attempt(10),
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
    """

    status_url = BASE_URL + "/getDownloadReportStatus"
    response = requests.get(url=status_url, params=[("referenceIds", report_reference_id)], cookies=cookie)
    json_data = response.json()
    status = json_data[0] if json_data else None

    logger.info("Executed Check Download status function. Status: ", status)

    return status


def download_ready_report(cookie: dict, report_reference_id: str, file_format: str = "TSV"):
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

    logger.info("Downloading AllOrders report...")
    try:
        download_url = BASE_URL + "/downloadFile"
        response = requests.get(
            url=download_url, params=[("referenceId", report_reference_id), ("fileFormat", file_format)], cookies=cookie
        )
        response.raise_for_status()
        logger.info(f"TSV data saved successfully for report with reference id:{report_reference_id} ")

    except Exception as e:
        logger.error(f"Failed to save data. Status code: {response.status_code}")
        return response.status_code, None

    return response.status_code, response.text


def convert_tsv_to_csv(tsv_data: str, output_file: str = None) -> None:
    """
    Convert TSV data to CSV format and save to file.

    Args:
        tsv_data: String containing TSV formatted data
        output_file: Path to save the output CSV file

    Raises:
        Exception: If there's an error during conversion or file saving
    """

    try:
        logger.info("Started converting TSV data to CSV file")
        # Read TSV data
        string_buffer = StringIO(tsv_data)
        df = pd.read_csv(string_buffer, sep="\t", encoding="utf-8")

        # Convert DataFrame to CSV string
        csv_data = df.to_csv(index=False, encoding="utf-8").encode('utf-8')
        
        # Save using the utility function
        file_path = save_content_to_file(
            content=csv_data,
            folder_name="allorders",
            file_name=output_file
        )

    except Exception as e:
        logger.error(f"Error converting file: {str(e)}")
        raise e


def download_allorder_report(report_start_date: str, report_end_date: str, client: str = "nexusbrand", 
                             brandname: str = "ExplodingKittens", bucket_name: str="rpa_validation_bucket"):
    """
    Download all orders report from Amazon Seller Central and upload to Google Cloud Storage.

    Args:
        report_start_date: Start date in YYYY/MM/DD format
        report_end_date: End date in YYYY/MM/DD format
        client: Client name for GCS path organization (default: "nexusbrand")
        brandname: Brand name for filename and GCS path (default: "ExplodingKittens")
        bucket_name: Google Cloud Storage bucket name (default: "rpa_validation_bucket")

    Raises:
        ValueError: If date parameters are invalid
        Exception: If any error occurs during download or upload process
    """

    try:

        validate_parameters(report_start_date, report_end_date)

        cookie = login_and_get_cookie()

        report_reference_id, report_status = request_allOrder_report(
            cookie=cookie,
            report_start_date=report_start_date,
            report_end_date=report_end_date
        )

        logger.info(f"Report Reference ID: {report_reference_id}    Report Status: {report_status}")

        if report_status != "Done":
            download_request_status = check_download_status(cookie=cookie, report_reference_id=report_reference_id)

            if download_request_status != "Done":
                logger.error("Maximum retry reached. Report can not be downloaded")
                return

        status_code, tsv_data = download_ready_report(cookie=cookie, report_reference_id=report_reference_id)

        if status_code == 200 and tsv_data != None:

            start_date_formatted = datetime.strptime(report_start_date, '%Y/%m/%d').strftime('%Y%m%d')
            end_date_formatted = datetime.strptime(report_end_date, '%Y/%m/%d').strftime('%Y%m%d')

            output_file = f"AllOrders_{brandname}_{start_date_formatted}_{end_date_formatted}.csv"
            convert_tsv_to_csv(tsv_data=tsv_data, output_file=output_file)
            
            # Extract year and month from report_end_date
            end_date = datetime.strptime(report_end_date, "%Y/%m/%d")
            destination_blob_name = f"UIReport/AmazonSellingPartner/{client}/{brandname}/AllOrders/year={end_date.strftime('%Y')}/month={end_date.strftime('%m')}/{output_file}"
            # upload_to_gcs(local_file_name=output_file, local_folder_name="allorders", bucket_name=bucket_name, destination_blob_name=destination_blob_name)

    except Exception as e:
        logger.error("Some Error ocurred while downloading")
        raise e


if __name__ == "__main__":
    args = parse_args(
        description='Download Amazon All Orders report for a date range',
        date_format='YYYY/MM/DD',
        optional_args=True
    )
    download_allorder_report(
        report_start_date=args.start_date,
        report_end_date=args.end_date,
        client=args.client,
        brandname=args.brandname,
        bucket_name=args.bucket_name
    )
