from azure.ai.ml import MLClient, load_component, Input
from azure.ai.ml.dsl import pipeline
from azure.identity import DefaultAzureCredential

SUBSCRIPTION_ID = "a485bb50-61aa-4b2f-bc7f-b6b53539b9d3"
RESOURCE_GROUP = "rg-60104281"
WORKSPACE_NAME = "tumour60104281"

ml_client = MLClient(
    DefaultAzureCredential(),
    subscription_id=SUBSCRIPTION_ID,
    resource_group_name=RESOURCE_GROUP,
    workspace_name=WORKSPACE_NAME,
)

feature_retrieval = load_component("components/feature_retrieval.yml")
feature_selection = load_component("components/feature_selection.yml")
train_and_eval = load_component("components/train_and_eval.yml")


@pipeline(default_compute="tumour-cpu-cluster")  
def tumor_gold_pipeline(features_input):
    a = feature_retrieval(features=features_input)
    b = feature_selection(train=a.outputs.train)
    c = train_and_eval(
        train=a.outputs.train,
        test=a.outputs.test,
        selected_features=b.outputs.selected_features,
    )
    return {
        "model": c.outputs.model,
        "metrics": c.outputs.metrics,
        "baseline_metrics": b.outputs.baseline_metrics,
        "ga_metrics": b.outputs.ga_metrics,
    }


if __name__ == "__main__":
    job = tumor_gold_pipeline(
        features_input=Input(
            type="uri_file",
            path="./data/tumour_features.parquet",
        )
    )

    job.experiment_name = "tumor_gold_pipeline"

    returned_job = ml_client.jobs.create_or_update(job)
    print("Pipeline submitted:")
    print(returned_job.studio_url)
