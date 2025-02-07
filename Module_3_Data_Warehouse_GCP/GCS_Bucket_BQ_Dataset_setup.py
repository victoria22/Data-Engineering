from google.cloud import storage, bigquery
from google.api_core.exceptions import Conflict

# Path to your Google Cloud credentials file
CREDENTIALS_FILE = "./keys/my-creds.json"  # Replace this with your credentials file

# Set the project and bucket name
PROJECT_ID = "dtc-de-course-448012"  
BUCKET_NAME = "dtc_de_zoomcamp_bucket"  
DATASET_ID = "dezoomcamp32"

# Initialize the client with the credentials file
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)

def create_bucket(project_id, bucket_name):
    """Creates a new GCS bucket."""
    try:
        # Create the new bucket
        bucket = client.bucket(bucket_name)

        # Create the bucket in your project with a specified location
        bucket = client.create_bucket(bucket, project=project_id, location="europe-west2")
        
        print(f"Bucket {bucket_name} created successfully in project {project_id}.")
    except Conflict:
        print(f"Bucket {bucket_name} already exists.")
    except Exception as e:
        print(f"Failed to create bucket: {e}")


def create_dataset(project_id, dataset_id):
    """Creates a new Big Query Dataset."""
    try:

        # Initialize BigQuery client
        bq_client = bigquery.Client(project=project_id)

        # Create the dataset object
        dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
        dataset.location = "europe-west2"  # Set the location for the dataset
        
        # Create the dataset in BigQuery
        dataset = bq_client.create_dataset(dataset)  # This sends the request to create the dataset
        
        print(f"Dataset {dataset_id} created successfully in project {project_id}.")
    except Conflict:
        print(f"Dataset {dataset_id} already exists.")
    except Exception as e:
        print(f"Failed to create dataset: {e}")


if __name__ == "__main__":
    create_bucket(PROJECT_ID, BUCKET_NAME)
    create_dataset(PROJECT_ID, DATASET_ID)
