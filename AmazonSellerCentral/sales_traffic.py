import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
from datetime import datetime
import requests
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_result
from auth import login_and_get_cookie
from helper.utils import save_content_to_file, parse_args, upload_to_gcs, reset_cookie
from helper.logging import logger
import yaml

COOKIE_STORAGE_PATH = Path(__file__).parent / "auth_state.json"
MARKET_PLACE_CONFIG_FILE_PATH = Path(__file__).parent / "report_config" / "market_place_config.yaml"
with open(MARKET_PLACE_CONFIG_FILE_PATH, "r") as file:
    market_place_config = yaml.safe_load(file)
marketplace_config = None

BASE_URL = None


def validate_parameters(report_start_date: str, report_end_date: str):
    """
    Validate the date parameters for the report.

    Args:
        report_start_date: Start date in YYYY-MM-DD format
        report_end_date: End date in YYYY-MM-DD format

    Raises:
        ValueError: If dates are in invalid format or if end_date is earlier than start_date
    """

    try:
        logger.info("Validating Parameters")

        start_date = datetime.strptime(report_start_date, "%Y-%m-%d")
        end_date = datetime.strptime(report_end_date, "%Y-%m-%d")
        if end_date < start_date:
            raise ValueError("End date cannot be earlier than start date")
    except ValueError as e:
        logger.error(f"Error Validating date: {str(e)}")
        raise e


def request_sales_traffic_report(report_start_date: str, report_end_date: str, cookie: dict, granularity: str = "DAY"):
    """
    Request sales and traffic report from Amazon Seller Central.

    Args:
        report_start_date: Start date in YYYY-MM-DD format
        report_end_date: End date in YYYY-MM-DD format
        granularity: Report granularity (DAY/WEEK/MONTH)
        cookie: Configured session cookie dict

    Returns:
        str: download_url if successful
        None: If the request fails

    Raises:
        requests.exceptions.RequestException: If there's an error making the request
    """

    logger.info("Requesting report download URL...")

    url = BASE_URL

    payload = {
        "operationName": "reportDataDownloadQuery",
        "variables": {
            "input": {
                "legacyReportId": "102:SalesTrafficTimeSeries",
                "startDate": report_start_date,
                "endDate": report_end_date,
                "granularity": granularity,
                "userSelectedRows": [],
                "selectedColumns": [
                    "SC_MA_Date_25913",
                    "SC_MA_OrderedProductSales_40591",
                    "SC_MA_UnitsOrdered_40590",
                    "SC_MA_TotalOrderItems_1",
                    "SC_MA_SalesPerOrderItem_1",
                    "SC_MA_UnitsPerOrderItem_1",
                    "SC_MA_AverageSellingPrice_25919",
                    "SC_MA_MobileAppPageViews",
                    "SC_MA_BrowserPageViews",
                    "SC_MA_PageViews_Total",
                    "SC_MA_MobileAppSessions",
                    "SC_MA_BrowserSessions",
                    "SC_MA_Sessions_Total",
                    "SC_MA_BuyBoxPercentage_25956",
                    "SC_MA_OrderItemSessionPercentage_1",
                    "SC_MA_UnitSessionPercentage_25957",
                    "SC_MA_AverageOfferCount_25954",
                    "SC_MA_AverageParentItems_25958",
                    "SC_MA_UnitsRefunded_25980",
                    "SC_MA_RefundRate_25981",
                    "SC_MA_FeedbackReceived_25982",
                    "SC_MA_NegativeFeedbackReceived_25983",
                    "SC_MA_ReceivedNegativeFeedbackRate_25984",
                    "SC_MA_AToZClaimsGranted_25985",
                    "SC_MA_ClaimsAmount_25986",
                    "SC_MA_ShippedProductSales_0002",
                    "SC_MA_UnitsShipped_0001",
                    "SC_MA_OrdersShipped_0001",
                ],
            }
        },
        "query": """
            query reportDataDownloadQuery($input: GetReportDataInput) {
                getReportDataDownload(input: $input) {
                    url
                    __typename
                }
            }
        """,
    }

    try:
        response = requests.post(url=url, json=payload, cookies=cookie)
        response.raise_for_status()

        json_response = response.json()
        download_url = json_response.get("data", {}).get("getReportDataDownload", {}).get("url")

        logger.info(f"Download URL fetched: {download_url}")
        return download_url

    except requests.exceptions.RequestException as e:
        logger.error(f"Error making request for Sales Traffic download url: {e}")
        return None


def download_report_data(
    download_url: str, report_start_date: str, report_end_date: str, cookie: dict, output_file: str
):
    """
    Download and save the sales traffic report locally.

    Args:
        download_url: URL to download the report
        report_start_date: Start date of the report in YYYY-MM-DD format
        report_end_date: End date of the report in YYYY-MM-DD format
        cookie: Session cookie dict for authentication
        output_file: Output filename for saving the report

    Returns:
        Path: Path object pointing to the saved file if successful
        None: If the download or save operation fails

    Raises:
        requests.exceptions.RequestException: If there's an error downloading the report
    """
    try:
        logger.info("Download URL obtained, started downloading...")
        response = requests.get(url=download_url, cookies=cookie)
        response.raise_for_status()

        logger.info(f"Report downloaded successfully, started saving")

        csv_data = response.content
        return csv_data

    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading report: {e}")
        return None


@retry(stop=stop_after_attempt(10), wait=wait_fixed(10), retry=retry_if_result(lambda result: result is None))
def download_sales_traffic_report(
    report_start_date: str,
    report_end_date: str,
    market_place: str,
    user_name: str,
    password: str,
    otp_secret: str,
    client: str = "nexusbrand",
    brandname: str = "ExplodingKittens",
    bucket_name: str = "rpa_validation_bucket",
) -> None:
    """
    Download sales and traffic report from Amazon Seller Central and upload to Google Cloud Storage.

    Args:
        report_start_date: Start date in YYYY-MM-DD format
        report_end_date: End date in YYYY-MM-DD format
        granularity: Report granularity (DAY/WEEK/MONTH)
        headless: Whether to run browser in headless mode
        client: Client name for GCS path organization
        brandname: Brand name for filename and GCS path organization
        bucket_name: Google Cloud Storage bucket name

    Returns:
        None

    Raises:
        ValueError: If date parameters are invalid
        Exception: If any error occurs during download or upload process
    """
    try:

        validate_parameters(report_start_date, report_end_date)

        cookie, headers = login_and_get_cookie(
            market_place=market_place, username=user_name, password=password, otp_secret=otp_secret, headless=False
        )

        download_url = request_sales_traffic_report(
            report_start_date=report_start_date, report_end_date=report_end_date, cookie=cookie
        )

        if download_url:
            start_date_formatted = datetime.strptime(report_start_date, "%Y-%m-%d").strftime("%Y%m%d")
            end_date_formatted = datetime.strptime(report_end_date, "%Y-%m-%d").strftime("%Y%m%d")
            output_file = f"SalesAndTraffic_{brandname}_{start_date_formatted}_{end_date_formatted}.csv"

            csv_data = download_report_data(
                download_url=download_url,
                report_start_date=report_start_date,
                report_end_date=report_end_date,
                cookie=cookie,
                output_file=output_file,
            )

            file_path = save_content_to_file(content=csv_data, folder_name="sales_traffic", file_name=output_file)

            if file_path:
                # Extract year and month from end_date
                end_date_obj = datetime.strptime(report_end_date, "%Y-%m-%d")
                year = end_date_obj.strftime("%Y")
                month = end_date_obj.strftime("%m")

                destination_blob_name = f"UIReport/AmazonSellingPartner/{client}/{brandname}/SalesAndTraffic/year={year}/month={month}/{output_file}"
                upload_to_gcs(
                    local_file_name=output_file,
                    local_folder_name="sales_traffic",
                    bucket_name=bucket_name,
                    destination_blob_name=destination_blob_name,
                )
            else:
                logger.error("Failed to download report")
                return None
        else:
            logger.error("Failed to get download URL")
            return None

        return destination_blob_name

    except Exception as e:
        logger.error("Some Error occurred while downloading: ")
        raise e


if __name__ == "__main__":
    args = parse_args(
        description="Download Sales and Traffic Report from Amazon Seller Central",
        date_format="YYYY-MM-DD",
        optional_args=True,
    )

    marketplace_config = market_place_config.get("marketplace_config", {}).get(args.market_place)
    BASE_URL = f"https://sellercentral.amazon.{marketplace_config["url_domain"]}/business-reports/api"

    reset_cookie(cookie_storage_path=COOKIE_STORAGE_PATH)

    download_sales_traffic_report(
        report_start_date=args.start_date,
        report_end_date=args.end_date,
        client=args.client,
        brandname=args.brandname,
        bucket_name=args.bucket_name,
        market_place=args.market_place,
        user_name=args.user_name,
        password=args.password,
        otp_secret=args.otp_secret,
    )
