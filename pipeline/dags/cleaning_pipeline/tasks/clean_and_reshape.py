from airflow.decorators import task
import pandas as pd

from deltalake import DeltaTable, Field, Schema, write_deltalake
from deltalake.exceptions import TableNotFoundError


@task(task_id="read_sensor_data_parquet")
def clean_and_reshape(parquet_file_path: str):
    # read input data
    df = pd.read_parquet(parquet_file_path)

    # pivot field and robot_id values into column
    df = df.pivot(
        index=["time", "run_uuid"], columns=["field", "robot_id"], values="value"
    )
    df.columns = ["_".join((field, str(robot_id))) for field, robot_id in df.columns]
    df = df.reset_index()

    # avoid being interpreted as scientific notation for run_uuid
    df["run_uuid"] = df["run_uuid"].apply("{:.0f}".format)

    # create output delta table if not already exists
    try:
        DeltaTable("data/pipeline_artifacts/cleaning_pipeline/clean_and_reshape")
    except TableNotFoundError:
        schema = Schema(
            [
                Field("run_uuid", "string"),
                Field("time", "string"),
                Field("x_1", "double"),
                Field("y_1", "double"),
                Field("z_1", "double"),
                Field("x_2", "double"),
                Field("y_2", "double"),
                Field("z_2", "double"),
                Field("fx_1", "double"),
                Field("fy_1", "double"),
                Field("fz_1", "double"),
                Field("fx_2", "double"),
                Field("fy_2", "double"),
                Field("fz_2", "double"),
            ]
        )
        DeltaTable.create(
            "data/pipeline_artifacts/cleaning_pipeline/clean_and_reshape",
            schema=schema,
            mode="error",
            partition_by=["run_uuid"],
        )

    # write output data to delta table
    write_deltalake(
        "data/pipeline_artifacts/cleaning_pipeline/clean_and_reshape",
        df,
        mode="overwrite",  # should be 'append' in production and use 'overwrite' if current run is backfill
    )
