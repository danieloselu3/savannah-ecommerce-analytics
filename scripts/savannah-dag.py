from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 10),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2
                             ),
}

# DAG Configuration
dag = DAG('ecommerce_dag',
          default_args=default_args,
          description='Extract, convert, and load user, cart, and product data',
          schedule_interval='0 2 * * *',
          catchup=False)

with dag:
    start = BashOperator(
        task_id='start',
        bash_command='echo "Starting the DAG execution"'
    )

    with TaskGroup("Extraction") as extraction_group:
        extract_users = BashOperator(
            task_id='extract_users',
            bash_command='python /home/airflow/gcs/dags/scripts/api-data-extraction.py '
                         '--url "https://dummyjson.com/users" --name "users" '
                         '--bucket_name "savannah-info-analytics-001-data-layers"',
        )

        extract_carts = BashOperator(
            task_id='extract_carts',
            bash_command='python /home/airflow/gcs/dags/scripts/api-data-extraction.py '
                         '--url "https://dummyjson.com/carts" --name "carts" '
                         '--bucket_name "savannah-info-analytics-001-data-layers"',
        )

        extract_products = BashOperator(
            task_id='extract_products',
            bash_command='python /home/airflow/gcs/dags/scripts/api-data-extraction.py '
                         '--url "https://dummyjson.com/products" --name "products" '
                         '--bucket_name "savannah-info-analytics-001-data-layers"',
        )

    with TaskGroup("Transformation") as transformation_group:
        convert_users = BashOperator(
            task_id='convert_users_to_csv',
            bash_command='python /home/airflow/gcs/dags/scripts/json-tocsv-conversion.py '
                         '--bucket_name "savannah-info-analytics-001-data-layers" '
                         '--source_blob_name "raw/users.json" '
                         '--destination_blob_name "cleanse/users.csv" '
                         '--data_type "users"',
        )

        convert_carts = BashOperator(
            task_id='convert_carts_to_csv',
            bash_command='python /home/airflow/gcs/dags/scripts/json-tocsv-conversion.py '
                         '--bucket_name "savannah-info-analytics-001-data-layers" '
                         '--source_blob_name "raw/carts.json" '
                         '--destination_blob_name "cleanse/carts.csv" '
                         '--data_type "carts"',
        )

        convert_products = BashOperator(
            task_id='convert_products_to_csv',
            bash_command='python /home/airflow/gcs/dags/scripts/json-tocsv-conversion.py '
                         '--bucket_name "savannah-info-analytics-001-data-layers" '
                         '--source_blob_name "raw/products.json" '
                         '--destination_blob_name "cleanse/products.csv" '
                         '--data_type "products"',
        )

    with TaskGroup("Loading") as loading_group:
        load_users = BashOperator(
            task_id='load_users_to_bq',
            bash_command='python /home/airflow/gcs/dags/scripts/user-bq-loader.py --input_file "gs://savannah-info-analytics-001-data-layers/cleanse/users.csv" '
                         '--dataset_id "ecommerce_data"',
        )

        load_carts = BashOperator(
            task_id='load_carts_to_bq',
            bash_command='python /home/airflow/gcs/dags/scripts/cart-bq-loader.py --input_file "gs://savannah-info-analytics-001-data-layers/cleanse/carts.csv" '
                         '--dataset_id "ecommerce_data"',
        )

        load_products = BashOperator(
            task_id='load_products_to_bq',
            bash_command='python /home/airflow/gcs/dags/scripts/product-bq-loader.py --input_file "gs://savannah-info-analytics-001-data-layers/cleanse/products.csv" '
                         '--dataset_id "ecommerce_data"',
        )

    failure_email = BashOperator(
        task_id='failure_notification',
        bash_command='echo "The DAG execution failed. Please check the logs."',
        trigger_rule='one_failed'
    )

    end = BashOperator(
        task_id='end',
        bash_command='echo "DAG Execution Success"'
    )

    start >> extraction_group >> transformation_group >> loading_group >> [end, failure_email]
