import os
import json
import requests
import argparse
from typing import Dict, List, Any
from google.cloud import storage
from datetime import datetime
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class APIDataExtractor:
    def __init__(self, bucket_name: str):
        self.storage_client = storage.Client()
        self.bucket_name = bucket_name

    def fetch_paginated_data(self, base_url: str, limit: int = 30, delay: float = 0.5) -> List[Dict[str, Any]]:
        all_items = []
        skip = 0
        total_fetched = 0

        while True:
            url = f"{base_url}?limit={limit}&skip={skip}"
            try:
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()

                # Extract items based on API structure
                items = data.get('products') or data.get('users') or data.get('carts') or []
                if not items:
                    break

                all_items.extend(items)
                total_fetched += len(items)
                if total_fetched >= data.get('total', 0):
                    break

                skip += limit
                time.sleep(delay)
                logger.info(f"Fetched {total_fetched} items so far")

            except requests.RequestException as e:
                logger.error(f"Error fetching data from {url}: {e}")
                break

        logger.info(f"Total items fetched: {total_fetched}")
        return all_items

    def save_to_gcs(self, data: List[Dict], filename: str, content_type: str = 'application/x-ndjson') -> bool:
        try:
            bucket = self.storage_client.bucket(self.bucket_name)
            blob = bucket.blob(filename)

            # Prepare NDJSON content
            extraction_timestamp = datetime.now().isoformat()
            ndjson_content = [
                json.dumps({"metadata": {"extraction_timestamp": extraction_timestamp}, "data": item})
                for item in data
            ]
            ndjson_string = '\n'.join(ndjson_content)
            blob.upload_from_string(ndjson_string, content_type=content_type)

            logger.info(f"Successfully uploaded {filename} to {self.bucket_name}")
            return True
        except Exception as e:
            logger.error(f"Error uploading to GCS: {e}")
            return False

def main():
    parser = argparse.ArgumentParser(description="Extract API data and upload to GCS")
    parser.add_argument('--url', required=True, help="API URL to extract data from")
    parser.add_argument('--name', required=True, help="Data name for file naming")
    parser.add_argument('--bucket_name', required=True, help="GCS bucket name")

    args = parser.parse_args()

    extractor = APIDataExtractor(bucket_name=args.bucket_name)
    data = extractor.fetch_paginated_data(args.url)
    filename = f"raw/{args.name}.json"
    extractor.save_to_gcs(data, filename)

if __name__ == "__main__":
    main()
