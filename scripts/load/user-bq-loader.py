import io
import argparse
import logging
import pandas as pd
from google.cloud import bigquery, storage

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class UsersBQLoader:
    def __init__(self, dataset_id: str):
        """
        Initialize BigQuery and GCS loaders for users table
        
        Args:
            dataset_id (str): BigQuery dataset ID
        """
        self.bq_client = bigquery.Client()
        self.gcs_client = storage.Client()
        self.dataset_id = dataset_id

    def read_gcs_csv(self, bucket_name: str, blob_name: str) -> pd.DataFrame:
        """
        Read CSV file directly from Google Cloud Storage
        
        Args:
            bucket_name (str): Name of the GCS bucket
            blob_name (str): Path to the CSV file in the bucket
        
        Returns:
            pd.DataFrame: Dataframe read from GCS
        """
        try:
            # Get the bucket and blob
            bucket = self.gcs_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            
            # Download the blob contents as a string
            csv_data = blob.download_as_text()
            
            # Read the CSV from the string
            df = pd.read_csv(io.StringIO(csv_data))
            
            logger.info(f"Successfully read CSV from gs://{bucket_name}/{blob_name}")
            return df
        
        except Exception as e:
            logger.error(f"Error reading CSV from GCS: {e}")
            raise

    def clean_users_table(self, input_file: str) -> pd.DataFrame:
        """
        Clean and normalize the Users table
        
        Args:
            input_file (str): Path to the input CSV file (in GCS format: gs://bucket-name/path/to/file.csv)
        
        Returns:
            pd.DataFrame: Cleaned users dataframe
        """
        try:
            # Parse the GCS path
            if not input_file.startswith('gs://'):
                raise ValueError("Input file must be a GCS path (gs://bucket-name/path)")
            
            # Remove 'gs://' and split into bucket and blob
            gcs_path = input_file[5:]
            bucket_name, blob_name = gcs_path.split('/', 1)
            
            # Read the CSV from GCS
            df = self.read_gcs_csv(bucket_name, blob_name)

            # Prepare the list of columns ensuring specific order
            # 1. SGK column
            sgk_columns = ['sgk_user_id',]
            
            # 2. Business columns
            business_columns = [
                'user_id', 
                'user_firstName', 
                'user_lastName', 
                'user_gender', 
                'user_age', 
                'user_address_address', 
                'user_address_city', 
                'user_address_postalCode',
            ]
            
            # 3. Audit columns
            audit_columns = [
                'record_create_name',
                'record_create_datetime',
                'record_update_name',
                'record_update_datetime',
                'source_system_code',
            ]
            
            # Combine all columns to ensure they exist
            all_required_columns = sgk_columns + business_columns + audit_columns
            
            # Create the carts table with selected columns
            users_table = df[all_required_columns].copy()
            
            # Rename columns to match target schema
            users_table.columns = [
                'sgk_user_id',
                'user_id', 
                'first_name', 
                'last_name', 
                'gender', 
                'age', 
                'street', 
                'city', 
                'postal_code',
                'record_create_name',
                'record_create_datetime',
                'record_update_name',
                'record_update_datetime',
                'source_system_code'
                ]

            
            # Basic data cleaning
            users_table['age'] = pd.to_numeric(users_table['age'], errors='coerce')
            users_table = users_table.dropna(subset=['user_id', 'first_name', 'last_name'])
            
            logger.info(f"Users table cleaned. Rows: {len(users_table)}")
            return users_table
        
        except Exception as e:
            logger.error(f"Error cleaning users table: {e}")
            raise

    def load_to_bigquery(self, dataframe: pd.DataFrame, table_name: str = 'users_table') -> None:
        """
        Load cleaned dataframe to BigQuery
        
        Args:
            dataframe (pd.DataFrame): Cleaned dataframe to load
            table_name (str): Name of the BigQuery table
        """
        try:
            # Construct the full table name
            table_id = f"{self.bq_client.project}.{self.dataset_id}.{table_name}"
            
            # Configure the job
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
            
            # Load the DataFrame to BigQuery
            job = self.bq_client.load_table_from_dataframe(
                dataframe, table_id, job_config=job_config
            )
            
            # Wait for the job to complete
            job.result()
            
            logger.info(f"Loaded {job.output_rows} rows to {table_id}")
        
        except Exception as e:
            logger.error(f"Error loading users table to BigQuery: {e}")
            raise

def main():
    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Load Users Data from GCS to BigQuery")
    parser.add_argument('--input_file', required=True, 
                        help="GCS path to input users CSV file (format: gs://bucket-name/path/to/file.csv)")
    parser.add_argument('--dataset_id', required=True, help="BigQuery dataset ID")
    
    # Parse arguments
    args = parser.parse_args()
    
    try:
        # Initialize the loader
        users_loader = UsersBQLoader(dataset_id=args.dataset_id)
        
        # Clean the users table from GCS
        users_table = users_loader.clean_users_table(args.input_file)
        
        # Load to BigQuery
        users_loader.load_to_bigquery(users_table)
        
        logger.info("Users data processing completed successfully")
    
    except Exception as e:
        logger.error(f"Failed to process users data: {e}")
        raise

if __name__ == "__main__":
    main()