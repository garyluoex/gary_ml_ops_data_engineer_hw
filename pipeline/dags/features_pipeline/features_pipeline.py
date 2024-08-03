from airflow.models.dag import DAG
from airflow.decorators import task
from deltalake import DeltaTable, write_deltalake


with DAG(
    "features_pipeline",
    description="Compute features on cleaned sensor data.",
) as dag:

    @task(task_id="compute_features")
    def compute_features(delta_path: str):
        dt = DeltaTable(delta_path)
        df = dt.to_pandas()

        df["vx_1"] = (df.x_1 - df.x_1.shift(1)) / (
            df.timestamp - df.timestamp.shift(1) / 1000000
        )  # milimeters per milisecond

        write_deltalake(
            "data/pipeline_artifacts/data_with_features",
            df,
            mode="overwrite",
            schema_mode="overwrite",
            partition_by=["run_uuid"],
        )

        return "Computing features"

    compute_features_task = compute_features(
        "data/pipeline_artifacts/interpolate_sensor_data"
    )
