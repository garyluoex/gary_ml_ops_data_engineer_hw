from airflow.models.dag import DAG
from cleaning_pipeline.tasks.read_sensor_data import (
    read_sensor_data_parquet,
)
from cleaning_pipeline.tasks.interpolate_sensor_data import interpolate_sensor_data

with DAG(
    "cleaning_pipeline",
    description="Clean, format and interpolate sensor data.",
) as dag:

    read_sample_sensor_data = read_sensor_data_parquet("data/sample.parquet")

    interpolate_data = interpolate_sensor_data(
        "data/pipeline_artifacts/read_sensor_data"
    )

    read_sample_sensor_data >> interpolate_data
