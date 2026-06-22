from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os

load_dotenv()

connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

container_name = "raw-data"
container_client = blob_service_client.get_container_client(container_name)

file = "data/raw/raw_jobs.csv"
blob_name = os.path.basename(file)

with open(file, "rb") as data:
    container_client.upload_blob(name=blob_name, data=data, overwrite=True)
    print(f"Uploaded {blob_name} successfully!")