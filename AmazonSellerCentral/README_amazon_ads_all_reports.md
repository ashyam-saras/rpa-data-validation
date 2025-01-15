# Amazon Ads All Reports Script Documentation

## amazon_ads_all_reports.py

### Description
This script downloads Amazon Ads reports for a specified date range and uploads them to Google Cloud Storage.

### Usage
```bash
python amazon_ads_all_reports.py --report_list <report_names> --start_date <start_date> --end_date <end_date> --market_place <market_place> --user_name <user_name> --password <password> --otp_secret <otp_secret> --client <client> --brandname <brandname> --bucket_name <bucket_name>
```

### Arguments
- `--report_list`: Comma-separated list of report names to download (e.g., "Sponsored Products Search term report,Sponsored Display Targeting report").
- `--start_date`: Start date for the reports in YYYY/MM/DD format.
- `--end_date`: End date for the reports in YYYY/MM/DD format.
- `--market_place`: Marketplace identifier (e.g., "United States", "Germany").
- `--user_name`: Amazon Seller Central username.
- `--password`: Amazon Seller Central password.
- `--otp_secret`: OTP secret for two-factor authentication.
- `--client` (optional): Client name for GCS path organization 
- `--brandname` (optional): Brand name for filename and GCS path 
- `--bucket_name` (optional): Google Cloud Storage bucket name (default: "rpa_validation_bucket").

### Example Command
```bash
python amazon_ads_all_reports.py --report_list "Sponsored Products Search term report,Sponsored Display Targeting report" --start_date "2023/01/01" --end_date "2023/01/31" --market_place "United States" --user_name "client_username" --password "client_password" --otp_secret "otp_secret" --client "client_name" --brand_name "brand_name"
```

### Report Names from YAML Config
The list of report names can also be retrieved from YAML config files. Ensure the config file is properly formatted and contains the necessary report names.

#### Amazon Ads Reports
- Sponsored Products Search term report (Date format: YYYY/MM/DD)
- Sponsored Display Targeting report (Date format: YYYY/MM/DD)
- Sponsored Brands Search term report (Date format: YYYY/MM/DD)
- Sponsored Products Advertised product report (Date format: YYYY/MM/DD)
- Sponsored Display Advertised product report (Date format: YYYY/MM/DD)
- Sponsored Brands Campaign report (Date format: YYYY/MM/DD)



# Amazon Fulfillment All Reports Script Documentation

## fulfillment_all_reports.py

### Description
This script downloads Amazon Fulfillment reports for a specified date range and uploads them to Google Cloud Storage.

### Usage
```bash
python fulfillment_all_reports.py --report_list <report_names> --start_date <start_date> --end_date <end_date> --market_place <market_place> --user_name <user_name> --password <password> --otp_secret <otp_secret> --client <client> --brandname <brandname> --bucket_name <bucket_name>
```

### Arguments
- `--report_list`: Comma-separated list of report names to download (e.g., "All Orders,FBA Customer Returns").
- `--start_date`: Start date for the reports in YYYY/MM/DD format.
- `--end_date`: End date for the reports in YYYY/MM/DD format.
- `--market_place`: Marketplace identifier (e.g., "United States", "Germany").
- `--user_name`: Amazon Seller Central username.
- `--password`: Amazon Seller Central password.
- `--otp_secret`: OTP secret for two-factor authentication.
- `--client` (optional): Client name for GCS path organization (default: "nexusbrand").
- `--brandname` (optional): Brand name for filename and GCS path (default: "ExplodingKittens").
- `--bucket_name` (optional): Google Cloud Storage bucket name (default: "rpa_validation_bucket").

### Example Command
```bash
python fulfillment_all_reports.py --report_list "All Orders,FBA Customer Returns" --start_date "2023/01/01" --end_date "2023/01/31" --market_place "United States" --user_name "client_username" --password "client_password" --otp_secret "otp_secret"
```

### Report Names from YAML Config
The list of report names can also be retrieved from YAML config files. Ensure the config file is properly formatted and contains the necessary report names.

#### Fulfillment Reports
- All Orders (Date format: YYYY/MM/DD)
- FBA Customer Returns (Date format: YYYY/MM/DD)
- FBA Inventory (Date format: YYYY/MM/DD)


# Sales and Traffic Report

## sales_traffic.py

### Description
This script downloads Amazon Sales and Traffic reports for a specified date range and uploads them to Google Cloud Storage.

## Usage
```bash
python sales_traffic.py  --start_date 2024-12-20 --end_date 2024-12-29 --market_place 'Italy' --user_name 'client_username' --password 'client_password' --otp_secret 'otp_secret'
```

### Arguments
- `--start_date`: Start date for the reports in YYYY-MM-DD format.
- `--end_date`: End date for the reports in YYYY-MM-DD format.
- `--market_place`: Marketplace identifier (e.g., "United States", "Germany").
- `--user_name`: Amazon Seller Central username.
- `--password`: Amazon Seller Central password.
- `--otp_secret`: OTP secret for two-factor authentication.
- `--client` (optional): Client name for GCS path organization.
- `--brandname` (optional): Brand name for filename and GCS path.
- `--bucket_name` (optional): Google Cloud Storage bucket name (default: "rpa_validation_bucket").


# Payment Transaction Report

## payment_transaction.py

### Description
This script downloads Amazon Payment reports for a specified date range and uploads them to Google Cloud Storage.

## Usage
```bash
python payment_transaction.py  --start_date 2024/12/20 --end_date 2024/12/29 --market_place 'Italy' --user_name 'client_username' --password 'client_password' --otp_secret 'otp_secret'
```

### Arguments
- `--start_date`: Start date for the reports in YYYY/MM/DD format.
- `--end_date`: End date for the reports in YYYY/MM/DD format.
- `--market_place`: Marketplace identifier (e.g., "United States", "Germany").
- `--user_name`: Amazon Seller Central username.
- `--password`: Amazon Seller Central password.
- `--otp_secret`: OTP secret for two-factor authentication.
- `--client` (optional): Client name for GCS path organization. 
- `--brandname` (optional): Brand name for filename and GCS path.
- `--bucket_name` (optional): Google Cloud Storage bucket name (default: "rpa_validation_bucket").

### Marketplaces from Config
The available marketplaces can be retrieved from the `market_place_config.yaml` file. Ensure the config file is properly formatted and contains the necessary marketplace information.

#### Available Marketplaces
- United States
- Germany
- France
- Italy
- Spain

### Additional Information
- Ensure that the `auth_state.json` file is removed before running the script to clear any existing cookies.
- The script will automatically handle login and cookie management.
- The downloaded reports will be saved locally and uploaded to the specified Google Cloud Storage bucket.
- **Logging**: The script uses a logging mechanism to capture detailed information about its execution. Logs include timestamps, log levels (INFO, WARNING, ERROR), and messages. The log file is saved in the same directory as the script and can be used to troubleshoot issues and verify the steps performed by the script.
- Incase of Script Failure due to maximum retry and network issue, try re-running the script. The Scripts Over-writes already present files with the same name, both in local directory and GCS Bucket.