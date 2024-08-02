from airflow.models.dag import DAG
from airflow.decorators import task

import pandas as pd

with DAG(
    "ml_ops_data_engineer_hw",
    description="Process posiion and force sensor data.",
) as dag:

    @task(task_id="read_sensor_data_parquet")
    def read_sensor_data_parquet(parquet_file_path: str):
        df = pd.read_parquet(parquet_file_path)
        print(df)
        return "Reading parquet file complete"

    read_sample_sensor_data = read_sensor_data_parquet("data/sample.parquet")
