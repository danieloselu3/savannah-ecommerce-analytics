import json
import csv
import os
import argparse
import pandas as pd
import hashlib
from datetime import datetime
from typing import Dict, Any, List
from google.cloud import storage

def read_from_gcs(gcs_path: str) -> List[str]:
    """Read contents of a file from Google Cloud Storage."""
    client = storage.Client()
    bucket_name, file_path = gcs_path.replace("gs://", "").split("/", 1)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    content = blob.download_as_text()
    return content.splitlines()

def write_to_gcs(dataframe: pd.DataFrame, bucket_name: str, destination_blob_name: str):
    """Write DataFrame to Google Cloud Storage as CSV."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    csv_data = dataframe.to_csv(index=False, quoting=csv.QUOTE_NONNUMERIC)
    blob.upload_from_string(csv_data, content_type='text/csv')

    print(f"Successfully saved CSV to {destination_blob_name}")

def add_audit_columns(df: pd.DataFrame, data_type: str) -> pd.DataFrame:
    """
    Add audit columns to the DataFrame based on the data type.
    
    :param df: Input DataFrame
    :param data_type: Type of data ('carts', 'users', 'products')
    :return: DataFrame with added audit columns
    """
    current_time = datetime.utcnow().isoformat()
    source_system = "PUBLIC_DUMMYJSON_API"
    
    # Add standard audit columns
    df['record_create_name'] = "Daniel Oselu"
    df['record_create_datetime'] = current_time
    df['record_update_name'] = "Daniel Oselu"
    df['record_update_datetime'] = current_time
    df['source_system_code'] = source_system

    # Add surrogate key based on data type
    if data_type == 'products':
        df['sgk_product_id'] = df['product_id'].apply(lambda x: hashlib.md5(str(x).encode()).hexdigest())
    elif data_type == 'users':
        df['sgk_user_id'] = df['user_id'].apply(lambda x: hashlib.md5(str(x).encode()).hexdigest())
    elif data_type == 'carts':
        df['sgk_cart_id'] = df.apply(lambda row: hashlib.md5(
            f"{row['user_id']}{row.get('product_id', '')}{row.get('cart_id', '')}".encode()).hexdigest(), axis=1)
    
    return df

def flatten_json(nested_json: Dict[str, Any], data_type: str, separator: str = '_') -> List[Dict[str, Any]]:
    """Flatten JSON based on data type with specific handling."""
    flatten_functions = {
        'carts': flatten_cart_json,
        'users': flatten_user_json,
        'products': flatten_product_json
    }
    
    flatten_func = flatten_functions.get(data_type, flatten_general_json)
    return flatten_func(nested_json, separator)

def flatten_general_json(nested_json: Dict[str, Any], separator: str = '_') -> List[Dict[str, Any]]:
    """Flatten a generic JSON object."""
    def internal_flatten(x: Any, name: str = '') -> Any:
        if isinstance(x, dict):
            flattened = {}
            for key, value in x.items():
                sub_results = internal_flatten(value, name + key + separator)
                if isinstance(sub_results, dict):
                    flattened.update(sub_results)
                else:
                    flattened[name + key] = sub_results
            return flattened
        elif isinstance(x, list):
            return [internal_flatten(item, name) for item in x]
        else:
            return x

    result = internal_flatten(nested_json)
    return [result] if isinstance(result, dict) else result

def flatten_cart_json(nested_json: Dict[str, Any], separator: str = '_') -> List[Dict[str, Any]]:
    """Flatten cart-specific JSON."""
    cart_data = nested_json.get('data', {})
    cart_info = {
        'cart_id': cart_data.get('id'),
        'user_id': cart_data.get('userId'),
        'total_cart_value': cart_data.get('total'),
        'discounted_total_cart_value': cart_data.get('discountedTotal'),
        'total_products': cart_data.get('totalProducts'),
        'total_quantity': cart_data.get('totalQuantity')
    }

    products = cart_data.get('products', [])
    expanded_rows = []

    for product in products:
        row = cart_info.copy()
        row.update({
            'product_id': product.get('id'),
            'product_title': product.get('title'),
            'product_price': product.get('price'),
            'product_quantity': product.get('quantity'),
            'product_total': product.get('total'),
            'product_discount_percentage': product.get('discountPercentage'),
            'product_discounted_total': product.get('discountedTotal'),
            'product_thumbnail': product.get('thumbnail')
        })
        expanded_rows.append(row)
    return expanded_rows

def flatten_user_json(nested_json: Dict[str, Any], separator: str = '_') -> List[Dict[str, Any]]:
    """Flatten user-specific JSON."""
    user_data = nested_json.get('data', {})
    
    # Extract all keys from the user data
    flattened_user = {
        f'user_{k}': v for k, v in user_data.items() 
        if isinstance(v, (str, int, float, bool))
    }
    
    # Handle nested objects and lists if needed
    for k, v in user_data.items():
        if isinstance(v, dict):
            for sub_k, sub_v in v.items():
                flattened_user[f'user_{k}_{sub_k}'] = sub_v
        elif isinstance(v, list):
            flattened_user[f'user_{k}_count'] = len(v)
    
    return [flattened_user]

def flatten_product_json(nested_json: Dict[str, Any], separator: str = '_') -> List[Dict[str, Any]]:
    """Flatten product-specific JSON."""
    product_data = nested_json.get('data', {})
    
    # Similar to user flattening, but specific to product structure
    flattened_product = {
        f'product_{k}': v for k, v in product_data.items() 
        if isinstance(v, (str, int, float, bool))
    }
    
    # Handle nested objects and lists
    for k, v in product_data.items():
        if isinstance(v, dict):
            for sub_k, sub_v in v.items():
                flattened_product[f'product_{k}_{sub_k}'] = sub_v
        elif isinstance(v, list):
            flattened_product[f'product_{k}_count'] = len(v)
    
    return [flattened_product]

def convert_json_to_csv(bucket_name: str, source_blob_name: str, destination_blob_name: str, data_type: str):
    """
    Convert NDJSON file to flattened CSV in Google Cloud Storage.
    
    :param bucket_name: Name of the GCS bucket
    :param source_blob_name: Path to the source NDJSON file
    :param destination_blob_name: Path for the output CSV file
    :param data_type: Type of data ('carts', 'users', 'products')
    """
    # Construct full GCS path
    full_gcs_path = f"gs://{bucket_name}/{source_blob_name}"
    
    # Read and process data
    flattened_data = []
    
    for line in read_from_gcs(full_gcs_path):
        try:
            json_data = json.loads(line.strip())
            
            # Validate data structure
            if not isinstance(json_data.get('data'), dict):
                print(f"Skipping line - 'data' is not an object: {line}")
                continue
            
            # Flatten entries
            flattened_entries = flatten_json(json_data, data_type)
            
            # Add metadata if present
            for entry in flattened_entries:
                metadata = json_data.get('metadata', {})
                for meta_key, meta_value in metadata.items():
                    entry[f'metadata_{meta_key}'] = meta_value
            
            flattened_data.extend(flattened_entries)
        
        except json.JSONDecodeError:
            print(f"Error decoding JSON in line: {line}")
            continue
    
    # Convert to DataFrame
    df = pd.DataFrame(flattened_data)
    
    # Add audit columns
    df = add_audit_columns(df, data_type)
    
    # Write to GCS
    write_to_gcs(df, bucket_name, destination_blob_name)
    
    print(f"Processed {len(flattened_data)} entries")
    print(f"CSV columns: {list(df.columns)}")

def main():
    """Main function to parse arguments and convert JSON to CSV."""
    parser = argparse.ArgumentParser(description="Convert JSON to CSV and upload to GCS")
    parser.add_argument('--bucket_name', required=True, help="GCS bucket name")
    parser.add_argument('--source_blob_name', required=True, help="Source file in GCS")
    parser.add_argument('--destination_blob_name', required=True, help="Destination CSV file in GCS")
    parser.add_argument('--data_type', required=True, 
                        choices=['carts', 'products', 'users'], 
                        help="Data type for handling")

    args = parser.parse_args()
    convert_json_to_csv(
        args.bucket_name, 
        args.source_blob_name, 
        args.destination_blob_name, 
        args.data_type
    )

if __name__ == '__main__':
    main()