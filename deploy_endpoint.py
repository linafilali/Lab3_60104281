# deploy_endpoint.py
import os
from azure.identity import DefaultAzureCredential
from azure.ai.ml import MLClient
from azure.ai.ml.entities import (
    ManagedOnlineEndpoint,
    ManagedOnlineDeployment,
    CodeConfiguration,
)
from azure.core.exceptions import ResourceNotFoundError

# These match your workspace
SUBSCRIPTION_ID = os.getenv("AZURE_SUBSCRIPTION_ID", "a485bb50-61aa-4b2f-bc7f-b6b53539b9d3")
RESOURCE_GROUP = os.getenv("AZURE_RESOURCE_GROUP", "rg-60104281")
WORKSPACE_NAME = os.getenv("AZURE_ML_WORKSPACE", "tumour60104281")

MODEL_NAME = "tumor_rf_model"          # registered in train_and_eval.py via mlflow.sklearn.log_model
ENDPOINT_NAME = "tumor-endpoint"       # must be lowercase, <= 32 chars
DEPLOYMENT_NAME = "blue"               # e.g. blue/green deployments


def get_ml_client() -> MLClient:
    cred = DefaultAzureCredential()
    return MLClient(
        credential=cred,
        subscription_id=SUBSCRIPTION_ID,
        resource_group_name=RESOURCE_GROUP,
        workspace_name=WORKSPACE_NAME,
    )


def get_latest_model(ml_client: MLClient):
    models = list(ml_client.models.list(name=MODEL_NAME))
    if not models:
        raise RuntimeError(f"No registered models found with name '{MODEL_NAME}'")

    # Pick highest version
    latest = sorted(models, key=lambda m: int(m.version))[-1]
    print(f"Using latest model: {latest.name} v{latest.version}")
    return latest


def ensure_endpoint(ml_client: MLClient):
    try:
        endpoint = ml_client.online_endpoints.get(ENDPOINT_NAME)
        print(f"Endpoint '{ENDPOINT_NAME}' already exists, will update it.")
    except ResourceNotFoundError:
        print(f"Endpoint '{ENDPOINT_NAME}' does not exist. Creating it...")
        endpoint = ManagedOnlineEndpoint(
            name=ENDPOINT_NAME,
            auth_mode="key",
            description="Brain tumor detection endpoint (feature-based classifier).",
        )
        endpoint = ml_client.online_endpoints.begin_create_or_update(endpoint).result()
        print(f"Created endpoint '{ENDPOINT_NAME}'.")
    return endpoint


def create_or_update_deployment(ml_client: MLClient, model_id: str):
    code_config = CodeConfiguration(
        code="./src",          # folder containing score.py
        scoring_script="score.py",
    )

    deployment = ManagedOnlineDeployment(
        name=DEPLOYMENT_NAME,
        endpoint_name=ENDPOINT_NAME,
        model=model_id,
        environment="azureml:AzureML-sklearn-1.0-ubuntu20.04-py38-cpu@latest",
        code_configuration=code_config,
        instance_type="Standard_DS3_v2",
        instance_count=1,
    )

    result = ml_client.online_deployments.begin_create_or_update(deployment).result()
    print(
        f"Deployment '{DEPLOYMENT_NAME}' updated. "
        f"Provisioning state: {result.provisioning_state}"
    )


def route_traffic(ml_client: MLClient):
    endpoint = ml_client.online_endpoints.get(ENDPOINT_NAME)
    endpoint.traffic = {DEPLOYMENT_NAME: 100}
    ml_client.online_endpoints.begin_create_or_update(endpoint).result()
    print(f"100% traffic routed to deployment '{DEPLOYMENT_NAME}'.")


def main():
    ml_client = get_ml_client()
    latest_model = get_latest_model(ml_client)
    ensure_endpoint(ml_client)
    create_or_update_deployment(ml_client, latest_model.id)
    route_traffic(ml_client)
    print(
        f"✅ Deployed model {latest_model.name}:{latest_model.version} "
        f"to endpoint '{ENDPOINT_NAME}'."
    )


if __name__ == "__main__":
    main()
