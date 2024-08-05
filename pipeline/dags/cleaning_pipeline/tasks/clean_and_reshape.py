import datetime
from airflow.decorators import task
import numpy as np
import pandas as pd

from deltalake import DeltaTable, Field, Schema, write_deltalake
from deltalake.exceptions import TableNotFoundError


# Assume sensor data will be delivered and parititioned by delivery datetime, old data could be delivered at a later date
# assume every record is delivered only once
# Assume the start of a batch is delivered first
# This task process latest sensor data by time interval and append data to delta table
# In the case of backfill, this task will overwrite using predicate where run__uuid and
@task(task_id="clean_and_reshape")
def clean_and_reshape(**kwargs):
    # create output delta table if not already exists
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
    try:
        DeltaTable("data/pipeline_artifacts/cleaning_pipeline/clean_and_reshape")
    except TableNotFoundError:
        DeltaTable.create(
            "data/pipeline_artifacts/cleaning_pipeline/clean_and_reshape",
            schema=schema,
            mode="error",
            partition_by=["run_uuid"],
        )

    data_interval_start = kwargs["dag_run"].data_interval_start
    data_interval_end = kwargs["dag_run"].data_interval_end

    assert data_interval_end - data_interval_start == datetime.timedelta(
        days=1
    ), "This task is desinged to process data one day at a time."

    # read input data
    input_dt = DeltaTable(
        "data/pipeline_artifacts/partition_pipeline/parition_sensor_data_by_run_uuid"
    )
    run_uuid_df = input_dt.to_pandas(
        columns=["run_uuid"],
        partitions=[("delivery_date", "=", data_interval_start.strftime("%Y%m%d"))],
    )
    distinct_run_uuids = run_uuid_df["run_uuid"].unique().tolist()

    for run_uuid in distinct_run_uuids:
        run_df = input_dt.to_pandas(
            partitions=[
                ("run_uuid", "=", run_uuid),
            ]
        )

        # pivot field and robot_id values into column
        run_df = run_df.pivot(
            index=["time", "run_uuid"], columns=["field", "robot_id"], values="value"
        )
        run_df.columns = [
            "_".join((field, str(robot_id))) for field, robot_id in run_df.columns
        ]
        run_df = run_df.reset_index()

        # if any missing columns, fill with null
        table_column_names = [
            field.name
            for field in schema.fields
            if field.name not in ["run_uuid", "time"]
        ]
        data_column_names = run_df.columns
        for missing_col_name in [
            col for col in table_column_names if col not in data_column_names
        ]:
            run_df[missing_col_name] = np.nan

        # write output data to delta table
        write_deltalake(
            "data/pipeline_artifacts/cleaning_pipeline/clean_and_reshape",
            run_df,
            mode="overwrite",
            partition_filters=[("run_uuid", "=", run_uuid)],
        )
