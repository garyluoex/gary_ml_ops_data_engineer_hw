from airflow.decorators import task
import pandas as pd

from deltalake import write_deltalake


@task(task_id="read_sensor_data_parquet")
def read_sensor_data_parquet(parquet_file_path: str):
    df = pd.read_parquet(parquet_file_path)

    df = df.pivot(
        index=["time", "run_uuid"], columns=["field", "robot_id"], values="value"
    )
    df.columns = ["_".join((field, str(robot_id))) for field, robot_id in df.columns]
    df = df.reset_index()
    df["run_uuid"] = df["run_uuid"].apply("{:.0f}".format)

    write_deltalake(
        "data/pipeline_artifacts/read_sensor_data",
        df,
        mode="overwrite",
        schema_mode="overwrite",
        partition_by=["run_uuid"],
    )
    return "Reading parquet file complete"
