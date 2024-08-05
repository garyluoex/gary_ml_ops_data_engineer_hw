import datetime

from airflow.decorators import task

import pandas as pd

from deltalake import DeltaTable, Field, Schema, write_deltalake
from deltalake.exceptions import TableNotFoundError


@task(task_id="parition_sensor_data_by_run_uuid")
def parition_sensor_data_by_run_uuid(**kwargs):
    external_trigger = kwargs["dag_run"].external_trigger
    run_type = kwargs["dag_run"].run_type
    data_interval_start = kwargs["dag_run"].data_interval_start
    data_interval_end = kwargs["dag_run"].data_interval_end

    assert (
        run_type == "scheduled" and not external_trigger
    ), "No manual trigger allowed for this pipeline to prevent potential data duplication."

    assert data_interval_end - data_interval_start == datetime.timedelta(
        days=1
    ), "This pipeline is desinged to process data one day at a time."

    df = pd.read_parquet(
        "data/source_sensor_data/",
        dtype_backend="pyarrow",
        filters=[
            (
                "delivery_date",
                "=",
                int(data_interval_start.strftime("%Y%m%d")),
            )
        ],
    )

    # convert from categorical type to string
    df["delivery_date"] = df["delivery_date"].astype(str)

    # convert from double to string and avoid being interpreted as scientific notation
    df["run_uuid"] = df["run_uuid"].apply("{:.0f}".format)

    # # create output delta table if not already exists
    try:
        DeltaTable(
            "data/pipeline_artifacts/partition_pipeline/parition_sensor_data_by_run_uuid"
        )
    except TableNotFoundError:
        schema = Schema(
            [
                Field("delivery_date", "string"),
                Field("run_uuid", "string"),
                Field("time", "string"),
                Field("value", "double"),
                Field("field", "string"),
                Field("robot_id", "long"),
                Field("sensor_type", "string"),
            ]
        )
        DeltaTable.create(
            "data/pipeline_artifacts/partition_pipeline/parition_sensor_data_by_run_uuid",
            schema=schema,
            mode="error",
            partition_by=["delivery_date", "run_uuid"],
        )

    write_deltalake(
        "data/pipeline_artifacts/partition_pipeline/parition_sensor_data_by_run_uuid",
        df,
        mode="append",
    )
