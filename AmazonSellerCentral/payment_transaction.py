import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
import requests
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_result
from helper.utils import save_content_to_file, parse_args, upload_to_gcs
from helper.logger import logger
from auth import login_and_get_cookie

BASE_URL = "https://sellercentral.amazon.com/payments/reports/api"


def validate_parameters(report_start_date: str, report_end_date: str):
    """
    Validate the date parameters for the report.

    Args:
        report_start_date (str): Start date in YYYY-MM-DD format
        report_end_date (str): End date in YYYY-MM-DD format

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


def request_report(cookie: dict, report_start_date: str, report_end_date: str) -> str:
    """
    Request a report from Amazon Seller Central.

    Args:
        cookie (dict): Authentication cookie
        report_start_date (str): Start date in YYYY/MM/DD format
        report_end_date (str): End date in YYYY/MM/DD format

    Returns:
        str: Report reference ID and report status
    """
    url = BASE_URL + "/request-report"

    start_date_timestamp = int((datetime.strptime(report_start_date, "%Y/%m/%d").timestamp()) * 1000)
    end_date_timestamp = int((datetime.strptime(report_end_date, "%Y/%m/%d").timestamp()) * 1000)

    start_date_iso = datetime.strptime(report_start_date, "%Y/%m/%d").strftime("%Y-%m-%dT%H:%M:%S+05:30")
    end_date_iso = datetime.strptime(report_end_date, "%Y/%m/%d").strftime("%Y-%m-%dT%H:%M:%S+05:30")

    print(start_date_timestamp, end_date_timestamp, start_date_iso, end_date_iso)

    data = {
        "accountType": "ALL",
        "reportType": "SELLER_TRANSACTION_DATE_RANGE",
        "startDate": start_date_timestamp,
        "startDateISO": start_date_iso,
        "endDate": end_date_timestamp,
        "endDateISO": end_date_iso,
        "timeRangeType": "CUSTOM",
    }

    try:

        logger.info("Requesting report...")
        response = requests.post(url, json=data, cookies=cookie)
        response.raise_for_status()
        response_json = response.json()
        logger.info(f"Report ID: {response_json['reportId']}")
        report_status = response_json.get("generatedReport", {}).get("status")
        report_reference_id = response_json["reportId"]
        return report_reference_id, report_status
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        return None, None


@retry(
    stop=stop_after_attempt(5),
    wait=wait_fixed(30),
    retry=retry_if_result(lambda result: result is None or result != "DOWNLOADABLE"),
)
def check_download_status(cookie: dict, report_reference_id: str) -> dict:
    """
    Check the download status of the requested report.

    Args:
        cookie (dict): Authentication cookie
        report_reference_id (str): Report reference ID

    Returns:
        dict: Report status
    """
    url = BASE_URL + "/report"
    params = {"reportId": report_reference_id}

    try:
        logger.info("Checking report status...")
        response = requests.get(url, params=params, cookies=cookie)
        response.raise_for_status()
        response_json = response.json()

        logger.info(f"Report status: {response_json['status']}")
        return response_json["status"]
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        return None


def download_report_data(cookie: dict, report_reference_id: str) -> str:
    """
    Download the report data from Amazon Seller Central.

    Args:
        cookie (dict): Authentication cookie
        report_reference_id (str): Report reference ID

    Returns:
        str: HTTP status code and report content
    """
    url = BASE_URL + "/download-report"
    params = {"reportId": report_reference_id}

    try:

        logger.info("Downloading report...")
        response = requests.get(url, params=params, cookies=cookie)
        response.raise_for_status()
        logger.info("Report downloaded successfully.")
        return response.status_code, response.content

    except requests.exceptions.RequestException as e:
        logger.error(f"Report Download failed: {e}")
        return None


def download_transaction_report(
    report_start_date: str,
    report_end_date: str,
    client: str = "nexusbrand",
    brandname: str = "ExplodingKittens",
    bucket_name: str = "rpa_validation_bucket",
):
    """
    Download payment Transaction report from Amazon Seller Central and upload to Google Cloud Storage.

    Args:
        report_start_date (str): Start date in YYYY/MM/DD format
        report_end_date (str): End date in YYYY/MM/DD format
        client (str): Client name for GCS path organization (default: "nexusbrand")
        brandname (str): Brand name for filename and GCS path (default: "ExplodingKittens")
        bucket_name (str): Google Cloud Storage bucket name (default: "rpa_validation_bucket")

    Raises:
        ValueError: If date parameters are invalid
        Exception: If any error occurs during download or upload process
    """

    try:

        validate_parameters(report_start_date, report_end_date)

        cookie, headers = login_and_get_cookie()

        report_reference_id, report_status = request_report(
            cookie=cookie, report_start_date=report_start_date, report_end_date=report_end_date
        )

        logger.info(f"Report Reference ID: {report_reference_id}    Report Status: {report_status}")

        if report_status != "DOWNLOADABLE":
            download_request_status = check_download_status(cookie=cookie, report_reference_id=report_reference_id)

            if download_request_status != "DOWNLOADABLE":
                logger.error("Maximum retry reached. Report can not be downloaded")
                return

        status_code, data = download_report_data(cookie=cookie, report_reference_id=report_reference_id)

        if status_code == 200 and data != None:

            start_date_formatted = datetime.strptime(report_start_date, "%Y/%m/%d").strftime("%Y%m%d")
            end_date_formatted = datetime.strptime(report_end_date, "%Y/%m/%d").strftime("%Y%m%d")

            output_file = f"PaymentTransaction_{brandname}_{start_date_formatted}_{end_date_formatted}.csv"
            # Save using the utility function
            file_path = save_content_to_file(content=data, folder_name="payment_transaction", file_name=output_file)

            # Extract year and month from report_end_date
            end_date = datetime.strptime(report_end_date, "%Y/%m/%d")
            destination_blob_name = f"UIReport/AmazonSellingPartner/{client}/{brandname}/PaymentTransaction/year={end_date.strftime('%Y')}/month={end_date.strftime('%m')}/{output_file}"
            upload_to_gcs(
                local_file_name=output_file,
                local_folder_name="payment_transaction",
                bucket_name=bucket_name,
                destination_blob_name=destination_blob_name,
            )

    except Exception as e:
        logger.error("Some Error ocurred while downloading")
        raise e


if __name__ == "__main__":

    args = parse_args(
        description="Download Amazon Payments Transaction reports for a date range",
        date_format="YYYY/MM/DD",
        optional_args=True,
    )

    download_transaction_report(
        report_start_date=args.start_date,
        report_end_date=args.end_date,
    )
