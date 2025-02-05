import sys
import csv
import json
from pathlib import Path
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))
import requests
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_result
import pandas as pd
from io import BytesIO, StringIO
from helper.utils import parse_args, save_content_to_file, upload_to_gcs
from helper.logging import logger
from auth2 import login_and_get_cookie
from datetime import datetime
import yaml

CONFIG_FILE_PATH = Path(__file__).parent / "report_config" / "retail_reports_config.yaml"
with open(CONFIG_FILE_PATH, "r") as file:
    config = yaml.safe_load(file)

MARKET_PLACE_CONFIG_FILE_PATH = Path(__file__).parent / "report_config" / "market_place_config.yaml"
with open(MARKET_PLACE_CONFIG_FILE_PATH, "r") as file:
    market_place_config = yaml.safe_load(file)
marketplace_config = None

BASE_URL = None

def load_report_from_yaml(
    report_name: str,
    market_place: str,
    report_start_date: str,
    report_end_date: str,
):
    """
    Load report configuration from a YAML file and update the start and end dates.

    Args:
        report_name: Name of the report to load.
        report_start_date: Start date in YYYY-MM-DD format.
        report_end_date: End date in YYYY-MM-DD format.

    Returns:
        A dictionary containing the report configuration, or None if the report is not found.
    """
    global BASE_URL
    global marketplace_config
    marketplace_config = market_place_config.get("marketplace_config", {}).get(market_place.split(' ')[0])
    BASE_URL = f'https://vendorcentral.amazon.{marketplace_config["url_domain"]}/api/retail-analytics/v1'

    # start_date_timestamp = int((datetime.strptime(report_start_date, "%Y-%m-%d").timestamp()) * 1000)
    # end_date_timestamp = int((datetime.strptime(report_end_date, "%Y-%m-%d").timestamp()) * 1000)

    report = config["retail_reports_config"].get(report_name, {})
    if report:
    # Access the first element of the dimensionSelections list
      for dimension in report["payload"]["reportRequest"]["dimensionSelections"]:
        if dimension.get("dimensionId") == "custom-period":
            dimension["value"]["startDate"] = report_start_date
            dimension["value"]["endDate"] = report_end_date

      return report
    return None


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
        logger.info(f"Error Validating date: {str(e)}")
        raise e


@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(5),
    retry=retry_if_result(lambda result: result[0] == 201 and result[1] is None),
)
def request_report(params: dict, payload: dict, cookie: dict, headers: dict):
    """
    Request the Retail reports from Amazon Vendor Central.

    Args:
        url: The URL to request the report.
        params: Query parameters for the request.
        payload: Request payload.

    Returns:
        A tuple containing the response status code and the report ID.
    """
    logger.info("Requesting Retail report content.")

    url = BASE_URL+ "/get-report-data"
    logger.info(url)

    # Query parameters
    params = params

    # Request payload
    payload = payload

    try:
        
        response = requests.post(url=url, cookies=cookie, headers=headers, json=payload)

        logger.info(f"Response Status: {response.status_code}")
        response.raise_for_status()
        #logger.info(f"Requested Content: {response.content}")

        return response.status_code, response.content.decode("utf-8")

    except Exception as e:
        logger.error(f"Error making request for requesting download of sponsored brand report: {e}")
        return None, None


@retry(stop=stop_after_attempt(3), wait=wait_fixed(5), retry=retry_if_result(lambda result: result is None))
def download_report_data(requested_content):
    """
    Download the report from the given requested content.

    Args:
        requested_content: The JSON data to download the report.

    Returns:
        The CSV data of the report, or None if the download fails.
    """
    try:
        logger.info("Started converting JSON data to CSV")

        # Extract the reportData from the JSON

        if isinstance(requested_content, str):
            try:
                json_data = json.loads(requested_content)
            except json.JSONDecodeError as decode_error:
                logger.error(f"Failed to decode JSON string: {decode_error}")
                return None
        report_data = json_data['reportData']


        # Extract columns and rows
    
        columns = [col['id'] for col in report_data['columns']]
        rows = report_data['rows']

        # Flatten rows into a 2D list
        flattened_rows = []
        for row in rows:
            flattened_rows.append([item.get('value', None) for item in row])

        # Create DataFrame
        df = pd.DataFrame(flattened_rows, columns=columns)

        # Save to CSV
        csv_data = df.to_csv(index=False, encoding='utf-8', quotechar='"', quoting=0)  # quoting=0 avoids putting quotes around data
        logger.info("CSV conversion successful")
        return csv_data
    

    
    except Exception as e:
        logger.error(f"Error downloading report: {e}")
        return None

import pandas as pd

def process_report(file_path, report_name, report_start_date, report_end_date, columns_to_clean=None, fill_value=0):
    """
    Process a report CSV file, adding a Date column and cleaning specified columns.
    
    :param file_path: Path to the CSV file.
    :param report_name: Name of the report.
    :param report_start_date: The start date of the report.
    :param report_end_date: The end date of the report.
    :param columns_to_clean: List of column names to clean (convert to numeric and handle NaN).
    :param fill_value: Value to fill NaN values with.
    """
    if file_path:
        if report_start_date == report_end_date:  # If start and end date are the same
            df = pd.read_csv(file_path)
            df.insert(0, "Date", pd.to_datetime(report_start_date))

            # Clean the specified columns
            if columns_to_clean:
                for col in columns_to_clean:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(fill_value).astype(int)
            
            # Save the modified DataFrame to CSV
            df.to_csv(file_path, index=False, quotechar='"')
            print(f"Processed {report_name} report successfully.")
        else:
            print(f"Start date and end date do not match for {report_name} report.")



def download_actual_report(
    report_start_date: str,
    report_end_date: str,
    params: dict,
    payload: dict,
    file_prefix: str,
    folder_name: str,
    retry_wait_time: int,
    client: str = "Nexusbrand",
    brandname: str = "ExplodingKittens",
    bucket_name: str = "rpa_validation_bucket",
    report_name: str = "Retail",
):
    """
    Download Retail reports from Amazon Vendor Central and upload to Google Cloud Storage.

    Args:
        report_start_date: Start date in YYYY-MM-DD format.
        report_end_date: End date in YYYY-MM-DD format.
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

        cookie, headers = login_and_get_cookie(
            amazon_retail=True,
            market_place=args.market_place,
            username=args.user_name,
            password=args.password,
            otp_secret=args.otp_secret,
            )

        report_status, requested_content = request_report(
            params=params, payload=payload, cookie=cookie, headers=headers
        )
        # logger.info(requested_content)

        if report_status == 200 :

            start_date_formatted = datetime.strptime(report_start_date, "%Y-%m-%d").strftime("%Y%m%d")
            end_date_formatted = datetime.strptime(report_end_date, "%Y-%m-%d").strftime("%Y%m%d")

            output_file = f"{file_prefix}_{start_date_formatted}_{end_date_formatted}.csv"
            csv_data = download_report_data(requested_content)
            csv_bytes = csv_data.encode('utf-8')
            # logger.info("CSV data: " + str(csv_bytes))
            file_path = save_content_to_file(content=csv_bytes, folder_name=folder_name, file_name=output_file)

            if file_path:
                if (report_name == "Sales" or report_name == "Traffic") and (report_start_date == report_end_date):
                    df = pd.read_csv(file_path)
                    df.insert(0, "Date", pd.to_datetime(report_start_date))
                    if report_name == "Sales":
                        # Handle non-finite values (NaN or inf) by filling them with 0 or another value
                        df['SHIPPED_UNITS'] = pd.to_numeric(df['SHIPPED_UNITS'], errors='coerce').fillna(0).astype(int)
                        
                    # Save the modified DataFrame to CSV
                    df.to_csv(file_path, index=False, quotechar='"')



                # Extract year and month from end_date
                end_date_obj = datetime.strptime(report_end_date, "%Y-%m-%d")
                year = end_date_obj.strftime("%Y")
                month = end_date_obj.strftime("%m")

                destination_blob_name = f"UIReports/AmazonVendorCentral/{client}/{file_prefix}/brandname={brandname}/year={year}/month={month}/{output_file}"
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

        return file_path

    except Exception as e:
        logger.error("Some Error occurred while downloading: ")
        raise e


if __name__ == "__main__":

    args = parse_args(
        description="Download Amazon Retail reports for a date range",
        date_format="yyyy-MM-dd",
        optional_args=True,
        amazon_retail=True,
    )

    report_list = args.report_list.split(",")

    for report_name in report_list:
        
        logger.info(f"GENERATING REPORT FOR {report_name}")
        report_config = load_report_from_yaml(
            report_name=report_name, report_start_date=args.start_date, report_end_date=args.end_date, market_place=args.market_place
        )

        params = report_config.get("params")
        payload = report_config.get("payload")
        folder_name = report_config.get("folder_name")
        file_prefix = report_config.get("file_prefix")
        retry_wait_time = report_config.get("retry_wait_time")

        download_actual_report(
            report_start_date=args.start_date,
            report_end_date=args.end_date,
            params=params,
            payload=payload,
            file_prefix=file_prefix,
            folder_name=folder_name,
            retry_wait_time=retry_wait_time,
            client=args.client,
            brandname=args.brandname,
            bucket_name=args.bucket_name,
            report_name=report_name,
        )
