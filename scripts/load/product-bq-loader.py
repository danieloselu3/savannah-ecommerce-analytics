import io
import argparse
import logging
import pandas as pd
from google.cloud import bigquery, storage

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ProductsBQLoader:
    def __init__(self, dataset_id: str):
        """
        Initialize BigQuery and GCS loaders for products table
        
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

    def clean_products_table(self, input_file: str) -> pd.DataFrame:
        """
        Clean and normalize the Products table
        
        Args:
            input_file (str): Path to the input CSV file (in GCS format: gs://bucket-name/path/to/file.csv)
        
        Returns:
            pd.DataFrame: Cleaned products dataframe
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
            sgk_columns = ['sgk_product_id',]
            
            # 2. Business columns
            business_columns = [
                'product_id', 
                'product_title', 
                'product_category', 
                'product_brand', 
                'product_price',
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
            products_table = df[all_required_columns].copy()
            
            # Rename columns to match target schema
            products_table.columns = [
                'sgk_product_id',
                'product_id', 
                'name', 
                'category', 
                'brand', 
                'price',
                'record_create_name',
                'record_create_datetime',
                'record_update_name',
                'record_update_datetime',
                'source_system_code'
                ]

            # Convert price to numeric and filter out low-value products
            products_table['price'] = pd.to_numeric(products_table['price'], errors='coerce')
            products_table = products_table[products_table['price'] > 50]
            
            logger.info(f"Products table cleaned. Rows: {len(products_table)}")
            return products_table
        
        except Exception as e:
            logger.error(f"Error cleaning products table: {e}")
            raise

    def load_to_bigquery(self, dataframe: pd.DataFrame, table_name: str = 'products_table') -> None:
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
            logger.error(f"Error loading products table to BigQuery: {e}")
            raise

def main():
    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Load Products Data from GCS to BigQuery")
    parser.add_argument('--input_file', required=True, 
                        help="GCS path to input products CSV file (format: gs://bucket-name/path/to/file.csv)")
    parser.add_argument('--dataset_id', required=True, help="BigQuery dataset ID")
    
    # Parse arguments
    args = parser.parse_args()
    
    try:
        # Initialize the loader
        products_loader = ProductsBQLoader(dataset_id=args.dataset_id)
        
        # Clean the products table from GCS
        products_table = products_loader.clean_products_table(args.input_file)
        
        # Load to BigQuery
        products_loader.load_to_bigquery(products_table)
        
        logger.info("Products data processing completed successfully")
    
    except Exception as e:
        logger.error(f"Failed to process products data: {e}")
        raise

if __name__ == "__main__":
    main()