from airflow.decorators import task
from deltalake import DeltaTable, Field, Schema, write_deltalake
import pandas as pd

from helpers import get_updated_run_uuids_in_data_interval


@task(task_id="interpolate_sensor_data")
def interpolate_sensor_data(**kwargs):
    distinct_run_uuids = get_updated_run_uuids_in_data_interval(
        kwargs["dag_run"].data_interval_start, kwargs["dag_run"].data_interval_end
    )

    input_pivoted_sensor_data_dt = DeltaTable(
        "data/pipeline_artifacts/cleaning_pipeline/clean_and_reshape"
    )
    # interpolate and write data to delta table for each run_uuid
    for run_uuid in distinct_run_uuids:
        input_run_df = input_pivoted_sensor_data_dt.to_pandas(
            partitions=[("run_uuid", "=", run_uuid)]
        )

        # compute timestamp from time for interpolation and other time related operations
        input_run_df["time_stamp"] = pd.DatetimeIndex(input_run_df.time).asi8
        input_run_df = input_run_df.set_index("time_stamp")

        # interpolate missing values scaled in reference to time_stamp
        input_run_df = input_run_df.interpolate(
            method="index", limit_direction="forward", axis="index"
        )

        # fill missing values at the beginning and end of the partition where interpolation is not possible
        input_run_df = input_run_df.ffill()
        input_run_df = input_run_df.bfill()

        # write output data to delta table
        write_deltalake(
            "data/pipeline_artifacts/cleaning_pipeline/interpolate_sensor_data",
            input_run_df,
            schema=_get_interpolate_sensor_data_output_schema(),
            mode="overwrite",
            partition_by=["run_uuid"],
            partition_filters=[("run_uuid", "=", run_uuid)],
        )


def _get_interpolate_sensor_data_output_schema():
    return Schema(
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
