import requests
from io import BytesIO
from google.cloud import storage
import os

CREDENTIALS_FILE = "./keys/my-creds.json"  
# Initialize Google Cloud Storage client
storage_client = storage.Client.from_service_account_json(CREDENTIALS_FILE)

# Define the GCS bucket where the data will be uploaded
bucket_name = "dtc-de-zoomcamp-bucket"  # Replace with your bucket name
bucket = storage_client.get_bucket(bucket_name)

# Define the base URL and months for 2019 data
base_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-{month:02d}.csv.gz'
months = range(1, 13)  # January to December

# Function to fetch and upload data for a specific month
def fetch_and_upload_data(month):
    url = base_url.format(month=month)
    response = requests.get(url)
    if response.status_code == 200:
        file_content = BytesIO(response.content)  # Convert content to BytesIO
        # Define the GCS path for each month
        gcs_path = f'fhv_tripdata_2019-{month:02d}.csv.gz'
        blob = bucket.blob(gcs_path)  # Create blob reference in GCS
        blob.upload_from_file(file_content, content_type="application/gzip")  # Upload to GCS
        print(f"Successfully uploaded data for month {month:02d} to GCS.")
    else:
        print(f"Failed to fetch data for month {month:02d}.")

# Fetch and upload data for each month
for month in months:
    fetch_and_upload_data(month)
