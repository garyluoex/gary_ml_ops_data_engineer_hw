from airflow.decorators import task

import pandas as pd

from deltalake import Field, Schema, write_deltalake


@task(task_id="partition_sensor_data")
def partition_sensor_data(**kwargs):
    input_sensor_data_df = pd.read_parquet(
        "data/source_sensor_data/",
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
        "{:.0f}".format
    )

    write_deltalake(
        "data/pipeline_artifacts/cleaning_pipeline/partition_sensor_data_by_run_uuid",
        input_sensor_data_df,
        schema=_get_partition_sensor_data_output_schema(),
        mode="append",
        partition_by=["delivery_date", "run_uuid"],
    )


def _get_partition_sensor_data_output_schema():
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
