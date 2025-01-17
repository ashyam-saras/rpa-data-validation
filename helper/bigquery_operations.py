from google.cloud import bigquery
import pandas as pd
from pathlib import Path
import os
from google.oauth2 import service_account


class BigQueryOperations:
    def __init__(self, credentials_path: str):
        """
        Initialize BigQuery client with local credentials

        Args:
            credentials_path: Path to service account JSON file
        """
        try:
            # Load credentials from the service account file
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )

            # Create BigQuery client with credentials
            self.client = bigquery.Client(credentials=credentials, project=credentials.project_id)
            print(f"Successfully connected to project: {credentials.project_id}")

        except Exception as e:
            print(f"Error initializing BigQuery client: {str(e)}")
            raise

    def create_table_from_csv(
        self,
        csv_path: str,
        dataset_id: str,
        table_id: str,
        schema: list = None,
        write_disposition: str = "WRITE_TRUNCATE",
    ) -> None:
        """
        Create or update BigQuery table from CSV file
        """
        try:
            # Verify CSV file exists
            if not os.path.exists(csv_path):
                raise FileNotFoundError(f"CSV file not found at: {csv_path}")

            # Configure the load job
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                autodetect=True if schema is None else False,
                schema=schema,
                write_disposition=write_disposition,
            )

            table_ref = f"{self.client.project}.{dataset_id}.{table_id}"

            # Load the CSV file into BigQuery
            with open(csv_path, "rb") as source_file:
                job = self.client.load_table_from_file(source_file, table_ref, job_config=job_config)

            # Wait for the job to complete
            job.result()

            print(f"Loaded {job.output_rows} rows into {table_ref}")

        except Exception as e:
            print(f"Error creating table from CSV: {str(e)}")
            raise

    def execute_stored_procedure(
        self,
        procedure_name: str,
        dataset_id: str,
        parameters: dict = None,
        output_dataset: str = None,
        output_table: str = None,
    ) -> pd.DataFrame:
        """
        Execute a stored procedure and optionally save results

        Args:
            procedure_name: Name of the stored procedure
            dataset_id: Dataset containing the stored procedure
            parameters: Dictionary of parameters to pass to the stored procedure
            output_dataset: Optional dataset to save results
            output_table: Optional table name to save results
        Returns:
            DataFrame containing the results
        """
        try:
            # Build the CALL statement with parameters if provided
            if parameters:
                param_str = ", ".join([f"@{k}={v}" for k, v in parameters.items()])
                query = f"CALL `{self.client.project}.{dataset_id}.{procedure_name}`({param_str})"
            else:
                query = f"CALL `{self.client.project}.{dataset_id}.{procedure_name}`()"

            # Configure job with destination if specified
            job_config = None
            if output_dataset and output_table:
                destination = f"{self.client.project}.{output_dataset}.{output_table}"
                job_config = bigquery.QueryJobConfig(destination=destination)

            # Execute the stored procedure
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()

            # Convert results to DataFrame
            df = results.to_dataframe()

            if output_dataset and output_table:
                print(f"Results saved to {destination}")

            return df

        except Exception as e:
            print(f"Error executing stored procedure: {str(e)}")
            raise

    def save_results_to_csv(self, df: pd.DataFrame, output_path: str, filename: str) -> str:
        """
        Save DataFrame to CSV file
        """
        try:
            # Ensure output directory exists
            Path(output_path).mkdir(parents=True, exist_ok=True)

            # Create full file path
            file_path = os.path.join(output_path, filename)

            # Save to CSV
            df.to_csv(file_path, index=False)
            print(f"Results saved to {file_path}")

            return file_path

        except Exception as e:
            print(f"Error saving results to CSV: {str(e)}")
            raise
