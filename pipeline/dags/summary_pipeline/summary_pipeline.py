from airflow.models.dag import DAG
from airflow.decorators import task
from deltalake import DeltaTable, write_deltalake


with DAG(
    "summary_pipeline",
    description="Create summaries using cleaned sensor data with features.",
) as dag:

    @task(task_id="calculate_runtime_statistics")
    def calculate_runtime_statistics(delta_path: str):
        dt = DeltaTable(delta_path)
        df = dt.to_pandas()

        df = df.groupby(["run_uuid"]).agg({"time_stamp": ["max", "min"]})

        write_deltalake(
            "data/pipeline_artifacts/summary_pipeline/run_statistics",
            df,
            mode="overwrite",
            schema_mode="overwrite",
        )

        return "Computing features"

    calculate_runtime_statistics_task = calculate_runtime_statistics(
        "data/pipeline_artifacts/features_pipeline/combined_features"
    )
