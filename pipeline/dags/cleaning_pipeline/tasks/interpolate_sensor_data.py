from airflow.decorators import task
from deltalake import DeltaTable, Field, Schema, write_deltalake
import pandas as pd
from deltalake.exceptions import TableNotFoundError


@task(task_id="interpolate_sensor_data")
def interpolate_sensor_data(input_delta_table_path: str):
    input_dt = DeltaTable(input_delta_table_path)

    # create output delta table if not already exists
    try:
        DeltaTable("data/pipeline_artifacts/cleaning_pipeline/interpolate_sensor_data")
    except TableNotFoundError:
        schema = Schema(
            [
                Field("run_uuid", "string"),
                Field("time", "string"),
                Field("time_stamp", "long"),
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
            "data/pipeline_artifacts/cleaning_pipeline/interpolate_sensor_data",
            schema=schema,
            mode="error",
            partition_by=["run_uuid"],
        )

    # get all run_uuid values so that we can process each run_uuid separately
    run_uuid_df = input_dt.to_pandas(columns=["run_uuid"])
    distinct_run_uuids = run_uuid_df["run_uuid"].unique().tolist()

    # interpolate and write data to delta table for each run_uuid
    for run_uuid in distinct_run_uuids:
        partition_df = input_dt.to_pandas(partitions=[("run_uuid", "=", run_uuid)])
        partition_df["time_stamp"] = pd.DatetimeIndex(partition_df.time).asi8
        partition_df = partition_df.set_index("time_stamp")

        # interpolate missing values using time_stamp
        partition_df = partition_df.interpolate(
            method="index", limit_direction="forward", axis="index"
        )

        # fill missing values at the beginning and end of the partition where interpolation is not possible
        partition_df = partition_df.ffill()
        partition_df = partition_df.bfill()

        # write output data to delta table
        write_deltalake(
            "data/pipeline_artifacts/cleaning_pipeline/interpolate_sensor_data",
            partition_df,
            mode="overwrite",
            partition_filters=[("run_uuid", "=", run_uuid)],
        )
