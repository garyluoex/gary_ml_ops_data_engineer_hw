from airflow.models.dag import DAG
from cleaning_pipeline.tasks.clean_and_reshape import (
    clean_and_reshape,
)
from cleaning_pipeline.tasks.interpolate_sensor_data import interpolate_sensor_data

with DAG(
    "cleaning_pipeline",
    description="Clean, format and interpolate sensor data.",
) as dag:

    clean_and_reshape_task = clean_and_reshape("data/sample.parquet")

    interpolate_task = interpolate_sensor_data(
        "data/pipeline_artifacts/cleaning_pipeline/clean_and_reshape"
    )

    clean_and_reshape_task >> interpolate_task
