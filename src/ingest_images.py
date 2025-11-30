import os
import argparse
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError

def get_blob_service_client():
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not conn_str:
        raise RuntimeError(
            "AZURE_STORAGE_CONNECTION_STRING environment variable not set. "
            "Please export it before running this script."
        )
    return BlobServiceClient.from_connection_string(conn_str)

def upload_dataset(local_data_dir: str, container_name: str, remote_prefix: str = "raw/tumour_images"):
    blob_service_client = get_blob_service_client()
    container_client = blob_service_client.get_container_client(container_name)

    try:
        container_client.create_container()
        print(f"Container '{container_name}' created.")
    except ResourceExistsError:
        print(f"Container '{container_name}' already exists. Using existing container.")

    for label in ["yes", "no"]:
        label_dir = os.path.join(local_data_dir, label)
        if not os.path.isdir(label_dir):
            print(f"WARNING: Directory not found: {label_dir}. Skipping label '{label}'.")
            continue

        for root, _, files in os.walk(label_dir):
            for filename in files:
                if filename.startswith("."):
                    continue

                local_path = os.path.join(root, filename)
                relative_name = os.path.basename(local_path)
                blob_path = f"{remote_prefix}/{label}/{relative_name}"

                blob_client = container_client.get_blob_client(blob_path)

                if blob_client.exists():
                    print(f"[SKIP] Blob already exists: {blob_path}")
                    continue

                print(f"[UPLOAD] {local_path} -> {blob_path}")
                with open(local_path, "rb") as data:
                    blob_client.upload_blob(data)

    print("Upload completed.")

def parse_args():
    parser = argparse.ArgumentParser(
        description="Ingest local tumor image dataset into ADLS Gen2 (Bronze layer)."
    )
    parser.add_argument(
        "--local-data-dir",
        required=True,
        help="Path to local dataset root containing 'yes' and 'no' subfolders.",
    )
    parser.add_argument(
        "--container-name",
        required=True,
        help="Name of the ADLS Gen2 container (e.g., 'tumorimages').",
    )
    parser.add_argument(
        "--remote-prefix",
        default="raw/tumour_images",
        help="Remote prefix inside the container. Default: 'raw/tumour_images'.",
    )
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    upload_dataset(
        local_data_dir=args.local_data_dir,
        container_name=args.container_name,
        remote_prefix=args.remote_prefix,
    )
