from decimal import Decimal
from airflow.decorators import task

import pandas as pd

from deltalake import Field, Schema, write_deltalake


def _get_partition_task_output_schema():
    return Schema(
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


@task(task_id="partition_task")
def partition_task(**kwargs):
    """
    Description: Paritions data by delivery_date and run_uuid to make downstream queries/processing more efficient.

    Input: Raw sensor data partitioned by 'delivery_date'. Amount of data retrieved is based on dag run data interval.

    Output: Raw sensor data with cleaned `delivery_date` and `run_uuid` columns paritioned by 'delivery_date' and 'run_uuid'.
            Amount of data processed and saved depends on dag run data interval.

    Reprocessing: Reprocess will cause duplicate data. Can update to allow reprocessing by adding predicate in 'write_deltalake' method.
    """
    input_sensor_data_df = pd.read_parquet(
        "data/source/",
        filters=[
            (
                "delivery_date",
                ">=",
                int(kwargs["dag_run"].data_interval_start.strftime("%Y%m%d")),
            ),
            (
                "delivery_date",
                "<",
                int(kwargs["dag_run"].data_interval_end.strftime("%Y%m%d")),
            ),
        ],
    )

    # convert from categorical dict type to string
    input_sensor_data_df["delivery_date"] = input_sensor_data_df[
        "delivery_date"
    ].astype(str)

    # convert from double to string and avoid being interpreted as scientific notation
    input_sensor_data_df["run_uuid"] = input_sensor_data_df["run_uuid"].apply(
        lambda val: "{:f}".format(Decimal(str(val)))
    )

    write_deltalake(
        "data/pipeline_artifacts/preprocess_pipeline/paritioned_data",
        input_sensor_data_df,
        schema=_get_partition_task_output_schema(),
        mode="append",
        partition_by=["delivery_date", "run_uuid"],
    )
