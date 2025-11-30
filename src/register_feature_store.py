
#
# "Feature store registration" step for Lab 5.
# In our workspace we don't have permissions to create a managed Feature Store
# resource, so instead we register the Silver parquet as a reusable Data asset
# called `tumor_features_silver`.
#
# This asset can then be referenced as:
#   azureml:tumor_features_silver:1
#
# from components or from pipeline_job.py.

from azure.identity import DefaultAzureCredential
from azure.ai.ml import MLClient
from azure.ai.ml.entities import Data
from azure.ai.ml.constants import AssetTypes
import os

# ---- Workspace config ----
SUBSCRIPTION_ID = os.getenv("AZURE_SUBSCRIPTION_ID", "a485bb50-61aa-4b2f-bc7f-b6b53539b9d3")
RESOURCE_GROUP = os.getenv("AZURE_RESOURCE_GROUP", "rg-60104281")
WORKSPACE_NAME = os.getenv("AZURE_ML_WORKSPACE", "tumour60104281")

# Local path to the Silver parquet generated in Phase 2
SILVER_PARQUET_PATH = "./data/tumour_features.parquet"
SILVER_ASSET_NAME = "tumor_features_silver"
SILVER_ASSET_VERSION = "1"


def get_ml_client() -> MLClient:
    cred = DefaultAzureCredential()
    return MLClient(
        credential=cred,
        subscription_id=SUBSCRIPTION_ID,
        resource_group_name=RESOURCE_GROUP,
        workspace_name=WORKSPACE_NAME,
    )


def main():
    ml_client = get_ml_client()

    # Define Data asset pointing to the Silver parquet
    data_asset = Data(
        name=SILVER_ASSET_NAME,
        version=SILVER_ASSET_VERSION,
        description="Silver-layer tumor MRI texture features (Parquet).",
        path=SILVER_PARQUET_PATH,
        type=AssetTypes.URI_FILE,
    )

    registered = ml_client.data.create_or_update(data_asset)
    print(
        f"Registered Silver features as Data asset: "
        f"{registered.name}:{registered.version}"
    )
    print("You can now reference it as: azureml:tumor_features_silver:1")


if __name__ == "__main__":
    main()
