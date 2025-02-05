import sys
import csv
import json
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
import requests
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_result
import pandas as pd
from io import StringIO
from helper.utils import parse_args, save_content_to_file, upload_to_gcs
from helper.logging import logger
from auth2 import login_and_get_cookie
from datetime import datetime
import yaml

CONFIG_FILE_PATH = Path(__file__).parent / "report_config" / "PO_reports_config.yaml"
with open(CONFIG_FILE_PATH, "r") as file:
    config = yaml.safe_load(file)


def load_report_from_yaml(
    report_name: str,
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

    start_date_timestamp = int((datetime.strptime(report_start_date, "%Y-%m-%d").timestamp()) * 1000)
    end_date_timestamp = int((datetime.strptime(report_end_date, "%Y-%m-%d").timestamp()) * 1000)

    report = config["PO_reports_config"].get(report_name, {})

    if report:
    # Access the first element of the dimensionSelections list
      payload = report.get("payload", {})
        
      # Replace the start_date and end_date placeholders
      payload_str = json.dumps(payload)  # Convert payload to string for manipulation
      payload_str = payload_str.replace("{start_date}", str(start_date_timestamp))
      payload_str = payload_str.replace("{end_date}", str(end_date_timestamp))
      report["payload"] = json.loads(payload_str)  # Convert back to dictionary

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
def request_report(url: str, params: dict, payload: dict, cookie: dict, headers: dict):
    """
    Request the purchase order report from Amazon Vendor Central.

    Args:
        url: The URL to request the report.
        params: Query parameters for the request.
        payload: Request payload.

    Returns:
        A tuple containing the response status code and the requested report content.
    """
    logger.info("Requesting Purchase Order report content.")

    url = url
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
        logger.error(f"Error making request for requesting download of purchase order report: {e}")
        return None, None

def timestamp_to_date(timestamp):
    return datetime.utcfromtimestamp(timestamp / 1000).strftime('%Y-%m-%d') if timestamp else ""


@retry(stop=stop_after_attempt(3), wait=wait_fixed(5), retry=retry_if_result(lambda result: result is None))
def download_report_data(requested_content):
    """
    Download the report from the given requested content.

    Args:
        requested_content: The json data to download the report.

    Returns:
        The CSV data of the report, or None if the download fails.
    """
    
    try:

        logger.info("Started converting JSON data to CSV")

        # Extract the payload from the JSON

        if isinstance(requested_content, str):
            try:
                json_data = json.loads(requested_content)
            except json.JSONDecodeError as decode_error:
                logger.error(f"Failed to decode JSON string: {decode_error}")
                return None
            
        report_data = json_data['payload']


        # Mapping dictionaries for Status and Freight terms
        STATUS_MAPPING = {
            "po_app_condition_closed": "Closed",
            "po_app_condition_confirmed": "Confirmed",
            # Add other mappings as needed
        }

        FREIGHT_TERMS_MAPPING = {
            "po_app_freight_collect": "Collect",
            # Add other mappings as needed
        }

        # Desired columns
        columns = [
            "PO", "Vendor code", "Order date", "Status", "PO items", "Requested quantity", 
            "Accepted quantity", "ASN quantity", "Received quantity", "Cancelled quantity", 
            "Remaining quantity", "Ship-to location", "Window start", "Window end", "Currency", 
            "Total requested cost", "Total accepted cost", "Total received cost", 
            "Total cancelled cost", "Freight terms", "Consolidation ID", "Cancellation deadline"
        ]

        # Generate CSV content in memory
        csv_buffer = StringIO()
        writer = csv.DictWriter(csv_buffer, fieldnames=columns)
        writer.writeheader()

        for record in report_data:
            submitted = record.get("submitted", {})
            accepted = record.get("accepted", {})
            asn = record.get("asn", {})
            received = record.get("received", {})
            cancelled = record.get("cancelled", {})

            # Calculate remaining quantity
            remaining_quantity = (
                submitted.get("quantity", 0) - 
                (accepted.get("quantity", 0) + cancelled.get("quantity", 0))
            )

            # Map Status and Freight terms
            status = STATUS_MAPPING.get(record.get("status"), record.get("status"))
            freight_terms = FREIGHT_TERMS_MAPPING.get(record.get("freightTerms"), record.get("freightTerms"))


            # Write row to CSV
            writer.writerow({
                "PO": record.get("poId"),
                "Vendor code": record.get("vendor"),
                "Order date": timestamp_to_date(record.get("orderDate")),
                "Status": status,
                "PO items": submitted.get("items"),
                "Requested quantity": submitted.get("quantity"),
                "Accepted quantity": accepted.get("quantity"),
                "ASN quantity": asn.get("quantity"),
                "Received quantity": received.get("quantity"),
                "Cancelled quantity": cancelled.get("quantity"),
                "Remaining quantity": remaining_quantity,
                "Ship-to location": record.get("shipLocation"),
                "Window start": timestamp_to_date(record.get("handOffStart")),
                "Window end": timestamp_to_date(record.get("handOffEnd")),
                "Currency": record.get("foreignCurrencyCode"),
                "Total requested cost": submitted.get("totalCost"),
                "Total accepted cost": accepted.get("totalCost"),
                "Total received cost": received.get("totalCost"),
                "Total cancelled cost": cancelled.get("totalCost"),
                "Freight terms": freight_terms,
                "Consolidation ID": record.get("consolidationId"),
                "Cancellation deadline": timestamp_to_date(record.get("cancellationDeadline")),
            })

        # Save CSV content to a file
        csv_data = csv_buffer.getvalue().encode("utf-8")
        logger.info("CSV conversion successful")
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
    client: str = "nexusbrand",
    brandname: str = "ExplodingKittens",
    bucket_name: str = "rpa_validation_bucket",
):
    """
    Download Purchase Order report from Amazon Vendor Central and upload to Google Cloud Storage.

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
            url=url, params=params, payload=payload, cookie=cookie, headers=headers
        )

        if report_status == 200 :

            start_date_formatted = datetime.strptime(report_start_date, "%Y-%m-%d").strftime("%Y%m%d")
            end_date_formatted = datetime.strptime(report_end_date, "%Y-%m-%d").strftime("%Y%m%d")

            output_file = f"{file_prefix}_{brandname}_{start_date_formatted}_{end_date_formatted}.csv"
            csv_data = download_report_data(requested_content)
            #csv_bytes = csv_data.decode('utf-8')
            file_path = save_content_to_file(content=csv_data, folder_name=folder_name, file_name=output_file)

            if file_path:
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
        description="Download Amazon VC Purchase Order reports for a date range",
        date_format="yyyy-MM-dd",
        optional_args=True,
        amazon_retail=True,
    )

    report_list = args.report_list.split(",")

    for report_name in report_list:
        
        logger.info(f"GENERATING REPORT FOR {report_name}")
        report_config = load_report_from_yaml(
            report_name=report_name, report_start_date=args.start_date, report_end_date=args.end_date
        )
        #print(f"report_config: {report_config}")

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
        )
