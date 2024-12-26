import requests
import json
from auth import login_and_get_cookie
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_result
import pandas as pd
from io import BytesIO
from utils import parse_args, save_content_to_file, upload_to_gcs
from datetime import datetime
from logger import logger
from amazon_ads_report_config import amazon_ads_report_config

# Add required headers
headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9',
        'anti-csrftoken-a2z': 'hAiJxLad0DQdfoXwP1gEdLashS0tVhswDEMa3V3O2a3nAAAAAGdqXmgAAAAB',
        'content-type': 'application/json',
        'cookie': 'session-id=133-1712160-8620223; ubid-main=130-9077621-0210530; kndctr_7742037254C95E840A4C98A6_AdobeOrg_identity=CiY4NTg0MjQ3MTcxODk1NDEwOTE4MzMyMDIzMzU2ODU2OTUxNDk3OFITCJTQ5Lu_MhABGAEqBElORDEwAPABlNDku78y; AMCV_7742037254C95E840A4C98A6%40AdobeOrg=MCMID|85842471718954109183320233568569514978; lc-main=en_US; session-id-time=2365744208l; at-main=Atza|IwEBIE7D4ZhEnOFOsQ2UR3KJk7XNHrWWCB3WISFh-8gD6aYbBIA0XsfRj5-FercbKNwUXmFArVvIaN9Z1VnCDfcl09u94lZFazlsYnrGfXA5oGyxxK_I9FFlu83Prmpq3FNWqR6d5Q1XNTKMk9P53BuLtH0MZuIJP7YXAvQXz2atXi8BZcnrZkW9m6xEO6om2Ht4I_0fLxtxEg3mU6-zQ05wo9AUiut2DWj7gMps9sRUlnA0-DhRkUE9VKaNmikJCmcwpPf4pON0W0t4tG8CkDfyWUbsJq8Wiq7OfLOGv90ki18liA; sess-at-main="ZjPNuxjtGZ/LfnagIs4YM1+Xr573zowm6yAvH3A3CvM="; sst-main=Sst1|PQHkz_Oo18iMdWabvQsjuWq8CYnsHsVNJkrwVB-_hYP9EJNVWnbP0_zv89ISDuoklLxx0WF2d9r7MQU31rsmC833kJRDbgunTNGwlsOfDCmWLqZQ1za0Xdl2mTZtxbweaLkynTEgwT3EnXwR7OUAkd8JyPVsPp_Z1L8fQFUT88c2WgE3zvmH-nqZSj2i3h6ra_fpinDBmNojCFX-An0ldK5nrTa3AzrwMO_I4IMi5KdxCg_hH_HIg9IILaqprdZzpvjyO4paCmunyHQCa6-jfgHE-H4Rzg7q_OjWK6qkSd1C9CQ; JSESSIONID=07A7591A2AEEB4A788F53267781D05C4; cwr_u=933b0684-45c2-4a4a-9b70-2ae63d5d3fb6; csm-hit=tb:CH3J8AGD6AE957XWPRCB+s-0P5R7VF5YN399HVKCTZF|1735024225341&t:1735024225341&adb:adblk_no; x-main=NI@hofLkhnv5?FRrZW2WKQGY9LMvb?JDreSjKhQLBqLxo59@uMZCX30E2hjkMfwa; session-token=w/9csS+tmjl6NF5uU+GguYZb1T0FCpMzxIYuXC0Z0skQaWujrB+jmXKxAnzL7pzy4CCjYde5nrDVcSxNt1YEgD2wTh6nG0kPiqJ1j9VCxqL7mrCmGYVZ/OB7BgoPR0fUEr4+lNXLrhb+K36LFT22FNWfKtRIvW5UuJh0rfC1a+93XHCKjvwporFx2zWaI8ED4zSbkjX28SQIeRL+YxRQZmJGshxRfR1/ENsGF26cyscaXgL3WaVo8/WUMBo3MSuCmWr/ySX619W+NHSsspN/1x2nwzobknoHXl27nXZdyL49AXdqLPUz5mv+ag8ZkdpDRgF2VMGKfShYSNFkL/NQ0GNHm96ZGDhYgMZo9fcHIHZtRLWdTf9GUAmR8mgK8zmo; cwr_s=eyJzZXNzaW9uSWQiOiIwZDZhODgxOC1iOTVmLTQwMWEtYjBiZC04YjQwNmU5MWQxMjkiLCJyZWNvcmQiOnRydWUsImV2ZW50Q291bnQiOjE2MiwicGFnZSI6eyJwYWdlSWQiOiIvcmVwb3J0cyIsInBhcmVudFBhZ2VJZCI6Ii9jbSIsImludGVyYWN0aW9uIjoxLCJyZWZlcnJlciI6Imh0dHBzOi8vYWR2ZXJ0aXNpbmcuYW1hem9uLmNvbS9jbS9jYW1wYWlnbnM/ZW50aXR5SWQ9RU5USVRZSzUyTjY5QUJDNk9ZIiwicmVmZXJyZXJEb21haW4iOiJhZHZlcnRpc2luZy5hbWF6b24uY29tIiwic3RhcnQiOjE3MzUwMjQyMzM4ODF9fQ==',
        'origin': 'https://advertising.amazon.com',
        'priority': 'u=1, i',
        'referer': 'https://advertising.amazon.com/reports/new?entityId=ENTITYK52N69ABC6OY',
        'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
    }

def update_report_dates(report_name, report_start_date, report_end_date):

    start_date_timestamp = int((datetime.strptime(report_start_date, "%Y/%m/%d").timestamp()) * 1000)
    end_date_timestamp = int((datetime.strptime(report_end_date, "%Y/%m/%d").timestamp()) * 1000)

    report = amazon_ads_report_config.get(report_name)
    if report:
        report["payload"]["reportStartDate"] = start_date_timestamp
        report["payload"]["reportEndDate"] = end_date_timestamp
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
    wait=wait_fixed(5),   #15 minutes
    retry=retry_if_result(lambda result: result[0] == 201 and result[1] is None) )
def fetch_sponsored_brands_report( url:str, params:dict, payload:dict):

    logger.info("Requesting sponsored brand report download url.")

    url = url
    
    # Query parameters
    params = params

    # Request payload
    payload = payload

    try:
        response = requests.put(
            url=url, 
            params=params, 
            json=payload, 
            headers=headers
        )
        
        logger.info(f"\nResponse Status: {response.status_code}")
        response.raise_for_status()
        logger.info(f"\nResponse Status: {response.status_code}")
        logger.info(f"Requested Report ID: {response.content}")

        return response.status_code, response.content.decode('utf-8')
        
    
    except Exception as e:
        logger.error(f"Error making request for requesting download of sponsored brand report: {e}")
        return None, None

@retry(
    stop=stop_after_attempt(10),
    wait=wait_fixed(30),   #15 minutes
    retry=retry_if_result(lambda result: result[0] != "COMPLETED" or result[1] is None))
def check_report_status(requested_report_id: str):
    
    try:
        logger.info("Checking report Status")

        url="https://advertising.amazon.com/reports/api/subscriptions?entityId=ENTITYK52N69ABC6OY"
    
        payload = {"filters":[{"column_name":"SUBSCRIPTION_ID","filter_type":"EQUAL","values":[requested_report_id]}]}

        response = requests.post(url=url, json=payload, headers=headers)

        response.raise_for_status()

        json_response = response.json()

        if json_response.get('subscriptions', [{}])[0].get('earliestUnprocessedReportSummary', {}):
            report_status = json_response.get('subscriptions', [{}])[0].get('earliestUnprocessedReportSummary', {}).get('status')
            report_download_url = None
        elif json_response.get('subscriptions', [{}])[0].get('latestProcessedReportSummary', {}):
            report_status = json_response.get('subscriptions', [{}])[0].get('latestProcessedReportSummary', {}).get('status')
            report_download_url = json_response.get('subscriptions', [{}])[0].get('latestProcessedReportSummary', {}).get('urlString')

        logger.info(f"Report Status:  {report_status}")
        return report_status,report_download_url
    
    except Exception as e:
        logger.error(f"Error downloading report: {e}")
        return None,None
    
@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(5),
    retry=retry_if_result(lambda result: result is None ))

def dowload_report(report_download_url: str, output_file: str, folder_name:str):

    try:
        logger.info("Started downloading")
        response = requests.get(url=report_download_url)

        response.raise_for_status()

        excel_file = BytesIO(response.content)
        df = pd.read_excel(excel_file, engine='openpyxl')
        csv_data = df.to_csv(index=False, encoding="utf-8").encode('utf-8')
        file_path = save_content_to_file(
                content=csv_data,
                folder_name=folder_name,
                file_name=output_file
            )
        return file_path
        
    except Exception as e:
        logger.error(f"Error downloading report: {e}")
        return None


def download_actual_report(
    report_start_date: str,
    report_end_date: str,
    url:str,
    params:dict,
    payload:dict,
    file_prefix:str,
    folder_name:str,
    client: str = "nexusbrand",
    brandname: str = "ExplodingKittens",
    bucket_name: str = "rpa_validation_bucket"
):
        """
    Download Sponsored Brand Campaign report from Amazon Ads and upload to Google Cloud Storage.

    Args:
        report_start_date: Start date in YYYY/MM/DD format
        report_end_date: End date in YYYY/MM/DD format
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

            # cookie = login_and_get_cookie()

            report_status, requested_report_id = fetch_sponsored_brands_report( url=url,params=params, payload=payload)
            # report_status, requested_report_id = 201, "c18854b9-a4cd-4b2d-8f98-ec38e89715d8"

            if report_status == 201 and requested_report_id:
                report_status, report_download_url = check_report_status(requested_report_id)

                if report_status == 'COMPLETED':
                    start_date_formatted = datetime.strptime(report_start_date, "%Y/%m/%d").strftime("%Y%m%d")
                    end_date_formatted = datetime.strptime(report_end_date, "%Y/%m/%d").strftime("%Y%m%d")
                    
                    output_file = f"{file_prefix}_{brandname}_{start_date_formatted}_{end_date_formatted}.csv"
                    file_path = dowload_report(report_download_url, output_file=output_file, folder_name=folder_name)

                    if file_path:
                    # Extract year and month from end_date  
                        end_date_obj = datetime.strptime(report_end_date, "%Y/%m/%d")
                        year = end_date_obj.strftime("%Y")
                        month = end_date_obj.strftime("%m")
                    
                        destination_blob_name = f"UIReport/AmazonSellingPartner/{client}/{brandname}/{file_prefix}/year={year}/month={month}/{output_file}"
                        upload_to_gcs(local_file_name=output_file, local_folder_name=folder_name, bucket_name=bucket_name, destination_blob_name=destination_blob_name)
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

    ##TODO: CHECK IF TIMESTAMP IS GETTING CONVERTED TO CORRECT DATES IN UI

    args = parse_args(
        description='Download Amazon Ads reports for a date range',
        date_format='YYYY/MM/DD',
        optional_args=True,
        amazon_ads=True
    )

    report_list = args.report_list.split(',')

    # report_list = [
    #                     "Sponsored Display Advertised product report", 
    #                     "Sponsored Products Advertised product report", 
    #                     "Sponsored Brands Search term report", 
    #                     "Sponsored Display Targeting report", 
    #                     "Sponsored Products Search term report",
    #                     # "Sponsored Brands Campaign report"   ##will take nearly 45 minutes
    #                     ]

    for report_name in report_list:
        
        logger.info(f"GENERATING REPORT FOR {report_name}")
        report_config = update_report_dates(report_name=report_name,report_start_date = "2024/11/28", report_end_date = "2024/12/24")
        
        url = report_config.get('url')
        params = report_config.get('params')
        payload = report_config.get('payload')
        folder_name = report_config.get('folder_name')
        file_prefix = report_config.get('file_prefix')
        
        download_actual_report(
                        report_start_date = args.start_date, 
                        report_end_date = args.end_date, 
                        url=url, 
                        params=params, 
                        payload=payload, 
                        file_prefix=file_prefix, 
                        folder_name=folder_name,
                        client=args.client,
                        brandname=args.brandname,
                        bucket_name=args.bucket_name
                        )