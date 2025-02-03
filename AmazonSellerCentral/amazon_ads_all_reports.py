import json
import re
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
import requests
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_result
import pandas as pd
from io import BytesIO
from helper.utils import parse_args, save_content_to_file, upload_to_gcs, reset_cookie
from helper.logging import logger
from auth import login_and_get_cookie
from datetime import datetime, timedelta
import yaml

BASE_URL = "https://advertising.amazon"
COOKIE_STORAGE_PATH = Path(__file__).parent / "auth_state.json"
CONFIG_FILE_PATH = Path(__file__).parent / "report_config" / "amazon_ads_report_config.yaml"
with open(CONFIG_FILE_PATH, "r") as file:
    config = yaml.safe_load(file)

MARKET_PLACE_CONFIG_FILE_PATH = Path(__file__).parent / "report_config" / "market_place_config.yaml"
with open(MARKET_PLACE_CONFIG_FILE_PATH, "r") as file:
    market_place_config = yaml.safe_load(file)
marketplace_config = None
entity_id = None


def load_report_from_yaml(
    report_name: str,
    report_start_date: str,
    report_end_date: str,
    market_place: str,
):
    """
    Load report configuration from a YAML file and update the start and end dates.

    Args:
        report_name: Name of the report to load.
        report_start_date: Start date in YYYY/MM/DD format.
        report_end_date: End date in YYYY/MM/DD format.
        market_place: Marketplace value to select the respective url domain and entity id.

    Returns:
        A dictionary containing the report configuration, or None if the report is not found.
    """

    start_date_timestamp = int(
        (((datetime.strptime(report_start_date, "%Y/%m/%d")) + timedelta(hours=5, minutes=30)).timestamp()) * 1000
    )
    end_date_timestamp = int(
        (((datetime.strptime(report_end_date, "%Y/%m/%d")) + timedelta(hours=5, minutes=30)).timestamp()) * 1000
    )

    logger.info(f"Report Start Date Timestamp: {start_date_timestamp}")
    logger.info(f"Report End Date Timestamp: {end_date_timestamp}")

    report = config.get("amazon_ads_report_config", {}).get(report_name)
    global marketplace_config
    marketplace_config = market_place_config.get("marketplace_config", {}).get(market_place)

    if report and marketplace_config:
        report["payload"]["reportStartDate"] = start_date_timestamp
        report["payload"]["reportEndDate"] = end_date_timestamp
        report["url"] = report["url"].format(url_domain=marketplace_config["url_domain"])
        report["params"]["entityId"] = marketplace_config["entityId"]
        return report
    return None


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
        logger.info(f"Error Validating date: {str(e)}")
        raise e


@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(5),
    retry=retry_if_result(lambda result: result[0] == 201 and result[1] is None),
)
def request_report(url: str, params: dict, payload: dict, cookie: dict, headers: dict):
    """
    Request the sponsored brand report from Amazon Ads.

    Args:
        url: The URL to request the report.
        params: Query parameters for the request.
        payload: Request payload.

    Returns:
        A tuple containing the response status code and the report ID.
    """
    logger.info("Requesting sponsored brand report download url.")

    url = url

    # Query parameters
    params = params

    # Request payload
    payload = payload

    global entity_id

    try:
        response = requests.get(
            url=f"https://sellercentral.amazon.{marketplace_config["url_domain"]}/global-dashboard/rest/v1/widgets/link-farm/settings?category=ACCOUNT_MANAGEMENT",
            cookies=cookie,
        )

        # Extract the value of 'encMerchantId'
        response_json = response.json()
        response_payload = json.loads(response_json["responsePayload"])
        enc_merchant_id = response_payload["merchantLevelDataResponses"][0]["encMerchantId"]
        logger.info(f"enc_merchant_id: {enc_merchant_id} ")

        locale = marketplace_config["locale"].replace("-", "_")
        response = requests.get(
            url=f"{BASE_URL}.{marketplace_config["url_domain"]}/reports/ref=xx_perftime_dnav_xx?merchantId={enc_merchant_id}&locale={locale}&ref=RedirectedFromSellerCentralByRoutingService",
            cookies=cookie,
        )
        response_text = response.text
        match = re.search(r'"entityId":\s?"([^"]+)"', response_text)
        if match:
            entity_id = match.group(1)
            logger.info(f"Entity ID: {entity_id}")
        else:
            logger.info("entityId not found.")

    except Exception as e:
        logger.error(f"Request failed while extracting entityID: {e}")
        return None, None

    try:
        params["entityId"] = entity_id
        response = requests.put(url=url, params=params, json=payload, cookies=cookie, headers=headers)

        logger.info(f"Response Status: {response.status_code}")
        response.raise_for_status()
        logger.info(f"Requested Report ID: {response.content}")

        return response.status_code, response.content.decode("utf-8")

    except Exception as e:
        logger.error(f"Error making request for requesting download of sponsored brand report: {e}")
        return None, None


def check_report_status(requested_report_id: str, cookie: dict, headers: dict, retry_wait_time: int = 30):
    """
    Check the status of the requested report.

    Args:
        requested_report_id: The ID of the requested report.
        retry_wait_time: Time to wait between retries in seconds.

    Returns:
        A tuple containing the report status and the download URL.
    """

    @retry(
        stop=stop_after_attempt(10),
        wait=wait_fixed(retry_wait_time),
        retry=retry_if_result(lambda result: result[0] != "COMPLETED" or result[1] is None),
    )
    def inner():
        try:
            logger.info("Checking report Status")

            url = f"{BASE_URL}.{marketplace_config["url_domain"]}/reports/api/subscriptions?entityId={entity_id}"

            payload = {
                "filters": [
                    {"column_name": "SUBSCRIPTION_ID", "filter_type": "EQUAL", "values": [requested_report_id]}
                ]
            }

            response = requests.post(url=url, json=payload, cookies=cookie, headers=headers)

            response.raise_for_status()

            json_response = response.json()

            if json_response.get("subscriptions", [{}])[0].get("earliestUnprocessedReportSummary", {}):
                report_status = (
                    json_response.get("subscriptions", [{}])[0]
                    .get("earliestUnprocessedReportSummary", {})
                    .get("status")
                )
                report_download_url = (
                    json_response.get("subscriptions", [{}])[0]
                    .get("latestProcessedReportSummary", {})
                    .get("urlString")
                )
            elif json_response.get("subscriptions", [{}])[0].get("latestProcessedReportSummary", {}):
                report_status = (
                    json_response.get("subscriptions", [{}])[0].get("latestProcessedReportSummary", {}).get("status")
                )
                report_download_url = (
                    json_response.get("subscriptions", [{}])[0]
                    .get("latestProcessedReportSummary", {})
                    .get("urlString")
                )

            logger.info(f"Report Status:  {report_status}")
            return report_status, report_download_url

        except Exception as e:
            logger.error(f"Error downloading report: {e}")
            return None, None

    return inner()


@retry(stop=stop_after_attempt(3), wait=wait_fixed(5), retry=retry_if_result(lambda result: result is None))
def download_report_data(report_download_url: str, cookie: dict):
    """
    Download the report from the given URL.

    Args:
        report_download_url: The URL to download the report.

    Returns:
        The CSV data of the report, or None if the download fails.
    """
    try:
        logger.info("Started downloading")
        url = f"{BASE_URL}.{marketplace_config["url_domain"]}" + report_download_url
        response = requests.get(url=url, cookies=cookie)

        response.raise_for_status()

        excel_file = BytesIO(response.content)
        df = pd.read_excel(excel_file, engine="openpyxl")
        csv_data = df.to_csv(index=False, encoding="utf-8").encode("utf-8")
        return csv_data

    except Exception as e:
        logger.error(f"Error downloading report: {e}")
        return None


def download_actual_report(
    report_start_date: str,
    report_end_date: str,
    url: str,
    params: dict,
    payload: dict,
    file_prefix: str,
    folder_name: str,
    retry_wait_time: int,
    cookie: dict,
    headers: dict,
    client: str = "nexusbrand",
    brandname: str = "ExplodingKittens",
    bucket_name: str = "rpa_validation_bucket",
):
    """
    Download Sponsored Brand Campaign report from Amazon Ads and upload to Google Cloud Storage.

    Args:
        report_start_date: Start date in YYYY/MM/DD format.
        report_end_date: End date in YYYY/MM/DD format.
        url: The URL to request the report.
        params: Query parameters for the request.
        payload: Request payload.
        file_prefix: Prefix for the output file name.
        folder_name: Folder name to save the file.
        retry_wait_time: Time to wait between retries in seconds.
        client: Client name.
        brandname: Brand name.
        bucket_name: Google Cloud Storage bucket name.

    Returns:
        The file path of the downloaded report, or None if the download fails.

    Raises:
        ValueError: If date parameters are invalid.
        Exception: If any error occurs during download or upload process.
    """
    try:
        validate_parameters(report_start_date, report_end_date)

        report_status, requested_report_id = request_report(
            url=url, params=params, payload=payload, cookie=cookie, headers=headers
        )

        if report_status == 201 and requested_report_id:
            report_status, report_download_url = check_report_status(
                requested_report_id, retry_wait_time=retry_wait_time, cookie=cookie, headers=headers
            )

            if report_status == "COMPLETED":
                start_date_formatted = datetime.strptime(report_start_date, "%Y/%m/%d").strftime("%Y%m%d")
                end_date_formatted = datetime.strptime(report_end_date, "%Y/%m/%d").strftime("%Y%m%d")

                output_file = f"{file_prefix}_{brandname}_{start_date_formatted}_{end_date_formatted}.csv"
                csv_data = download_report_data(report_download_url, cookie)
                file_path = save_content_to_file(content=csv_data, folder_name=folder_name, file_name=output_file)

                if file_path:
                    # Extract year and month from end_date
                    end_date_obj = datetime.strptime(report_end_date, "%Y/%m/%d")
                    year = end_date_obj.strftime("%Y")
                    month = end_date_obj.strftime("%m")

                    destination_blob_name = f"UIReport/AmazonSellingPartner/{client}/{brandname}/{file_prefix}/year={year}/month={month}/{output_file}"
                    upload_to_gcs(
                        local_file_name=output_file,
                        local_folder_name=folder_name,
                        bucket_name=bucket_name,
                        destination_blob_name=destination_blob_name,
                    )
                else:
                    logger.error("Failed to download report")
                    return None

            else:
                return None
        else:
            return None

        return file_path

    except Exception as e:
        logger.error("Some Error occurred while downloading: ")
        raise e


if __name__ == "__main__":

    args = parse_args(
        description="Download Amazon Ads reports for a date range",
        date_format="YYYY/MM/DD",
        optional_args=True,
        amazon_ads=True,
    )

    report_list = args.report_list.split(",")

    cookie = {}
    headers = {}

    while len(cookie) == 0 or len(headers) == 0:
        reset_cookie(cookie_storage_path=COOKIE_STORAGE_PATH)

        cookie, headers = login_and_get_cookie(
            amazon_ads=True,
            market_place=args.market_place,
            username=args.user_name,
            password=args.password,
            otp_secret=args.otp_secret,
            account=args.account,
        )

    for report_name in report_list:

        logger.info(f"GENERATING REPORT FOR {report_name}")

        report_config = load_report_from_yaml(
            report_name=report_name,
            report_start_date=args.start_date,
            report_end_date=args.end_date,
            market_place=args.market_place,
        )

        url = report_config.get("url")
        params = report_config.get("params")
        payload = report_config.get("payload")
        folder_name = report_config.get("folder_name")
        file_prefix = report_config.get("file_prefix")
        retry_wait_time = report_config.get("retry_wait_time")

        download_actual_report(
            report_start_date=args.start_date,
            report_end_date=args.end_date,
            url=url,
            params=params,
            payload=payload,
            file_prefix=file_prefix,
            folder_name=folder_name,
            retry_wait_time=retry_wait_time,
            client=args.client,
            brandname=args.brandname,
            bucket_name=args.bucket_name,
            cookie=cookie,
            headers=headers,
        )
