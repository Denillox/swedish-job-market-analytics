import os
from pathlib import Path

from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv


RAW_FILE = "data/raw/raw_jobs.csv"
EXPORTS_DIRECTORY = "data/exports"

PROCESSED_DIRECTORIES = [
    "data/processed/jobs.parquet",
    "data/processed/job_skills.parquet",
    "data/processed/skill_counts.parquet",
    "data/processed/location_counts.parquet",
    "data/processed/workplace_type_counts.parquet",
    "data/processed/employer_counts.parquet",
    "data/processed/experience_counts.parquet",
    "data/processed/years_experience_counts.parquet",
]


def upload_file(container_client, local_path, blob_name):
    local_path = Path(local_path)

    if not local_path.exists():
        raise FileNotFoundError(f"File not found: {local_path}")

    with open(local_path, "rb") as data:
        container_client.upload_blob(
            name=blob_name,
            data=data,
            overwrite=True
        )

    print(f"Uploaded {local_path} -> {blob_name}")

def upload_csv_files_from_directory(container_client, local_dir, blob_prefix):
    local_dir_path = Path(local_dir)

    if not local_dir_path.exists():
        raise FileNotFoundError(f"Exports directory not found: {local_dir}")

    csv_files = list(local_dir_path.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in: {local_dir}")

    for csv_file in csv_files:
        blob_name = f"{blob_prefix}/{csv_file.name}"

        upload_file(
            container_client=container_client,
            local_path=csv_file,
            blob_name=blob_name
        )

def upload_directory(container_client, local_dir, blob_prefix):
    local_dir_path = Path(local_dir)

    for root, _, files in os.walk(local_dir_path):
        for file_name in files:
            local_file_path = Path(root) / file_name

            relative_path = local_file_path.relative_to(local_dir_path)
            blob_name = f"{blob_prefix}/{relative_path}".replace("\\", "/")

            upload_file(container_client, local_file_path, blob_name)


def main():
    load_dotenv()

    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    container_name = os.getenv("AZURE_CONTAINER_NAME")

    if not connection_string:
        raise ValueError("Missing AZURE_STORAGE_CONNECTION_STRING")

    if not container_name:
        raise ValueError("Missing AZURE_CONTAINER_NAME")

    blob_service_client = BlobServiceClient.from_connection_string(
        connection_string
    )

    container_client = blob_service_client.get_container_client(container_name)

    upload_file(
        container_client=container_client,
        local_path=RAW_FILE,
        blob_name="raw/raw_jobs.csv"
    )

    for directory in PROCESSED_DIRECTORIES:
        dataset_name = Path(directory).name

        upload_directory(
            container_client=container_client,
            local_dir=directory,
            blob_prefix=f"processed/{dataset_name}"
        )
    
    upload_csv_files_from_directory(
        container_client=container_client,
        local_dir=EXPORTS_DIRECTORY,
        blob_prefix="exports"
    )

    print("Upload complete.")


if __name__ == "__main__":
    main()