# uv run --with google-cloud-storage --with requests taxi_download_gcp_upload.py
import os
import json
import requests
import gzip
import shutil
from google.cloud import storage
from google.oauth2 import service_account

def get_gcs_client():
    service_account_info = os.environ.get('GCP_SERVICE_ACCOUNT_JSON')
    if not service_account_info:
        raise ValueError("Secret GCP_SERVICE_ACCOUNT_JSON not found in environment")
    
    info = json.loads(service_account_info)
    credentials = service_account.Credentials.from_service_account_info(info)
    return storage.Client(credentials=credentials)

def download_and_upload(years, taxi_types, bucket_name):
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    # New Base URL for GitHub Releases
    base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
    
    for year in years:
        for taxi in taxi_types:
            for month in range(1, 13):
                # GitHub uses .csv.gz format
                source_file = f"{taxi}_tripdata_{year}-{month:02d}.csv.gz"
                # Target name in GCS (unzipped)
                target_file = f"{taxi}_tripdata_{year}-{month:02d}.csv"
                
                # Note: DataTalksClub repo uses taxi type as the release tag
                url = f"{base_url}/{taxi}/{source_file}"
                
                print(f"Processing {source_file}...")
                
                r = requests.get(url, stream=True)
                if r.status_code == 200:
                    # Temporary local save for the compressed file
                    with open(source_file, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)
                    
                    # Decompress the file
                    print(f"Unzipping {source_file}...")
                    with gzip.open(source_file, 'rb') as f_in:
                        with open(target_file, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    
                    # Upload the unzipped CSV to GCS
                    blob = bucket.blob(f"{target_file}")
                    blob.upload_from_filename(target_file)
                    print(f"Successfully uploaded {target_file}")
                    
                    # Clean up local files
                    os.remove(source_file)
                    os.remove(target_file)
                else:
                    print(f"Failed to find {url} (Status: {r.status_code})")

if __name__ == "__main__":
    download_and_upload([2019, 2020], ["green", "yellow"], "data-lake-bucket-2026")