import datetime
from airflow.decorators import task
from deltalake import DeltaTable, Field, Schema, write_deltalake
import pandas as pd
from deltalake.exceptions import TableNotFoundError


@task(task_id="interpolate_sensor_data")
def interpolate_sensor_data(**kwargs):
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

    data_interval_start = kwargs["dag_run"].data_interval_start
    data_interval_end = kwargs["dag_run"].data_interval_end
    assert data_interval_end - data_interval_start == datetime.timedelta(
        days=1
    ), "This pipeline is desinged to process data one day at a time."

    # read input data
    run_uuid_dt = DeltaTable(
        "data/pipeline_artifacts/partition_pipeline/parition_sensor_data_by_run_uuid"
    )
    run_uuid_df = run_uuid_dt.to_pandas(
        columns=["run_uuid"],
        partitions=[("delivery_date", "=", data_interval_start.strftime("%Y%m%d"))],
    )
    distinct_run_uuids = run_uuid_df["run_uuid"].unique().tolist()

    input_dt = DeltaTable("data/pipeline_artifacts/cleaning_pipeline/clean_and_reshape")
    # interpolate and write data to delta table for each run_uuid
    for run_uuid in distinct_run_uuids:
        run_df = input_dt.to_pandas(partitions=[("run_uuid", "=", run_uuid)])

        run_df["time_stamp"] = pd.DatetimeIndex(run_df.time).asi8
        run_df = run_df.set_index("time_stamp")

        # interpolate missing values scaled in reference to time_stamp
        run_df = run_df.interpolate(
            method="index", limit_direction="forward", axis="index"
        )

        # fill missing values at the beginning and end of the partition where interpolation is not possible
        run_df = run_df.ffill()
        run_df = run_df.bfill()

        print(run_df.info())
        # write output data to delta table
        write_deltalake(
            "data/pipeline_artifacts/cleaning_pipeline/interpolate_sensor_data",
            run_df,
            mode="overwrite",
            partition_filters=[("run_uuid", "=", run_uuid)],
        )
