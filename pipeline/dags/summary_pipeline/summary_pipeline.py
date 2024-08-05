import datetime
from airflow.models.dag import DAG
from airflow.decorators import task
from deltalake import DeltaTable, Field, Schema, write_deltalake
import pandas as pd

from helpers import get_updated_run_uuids_in_data_interval

with DAG(
    "summary_pipeline",
    description="Create summaries using cleaned sensor data with features.",
    schedule=datetime.timedelta(days=1),
    start_date=datetime.datetime(2022, 11, 23),
    end_date=datetime.datetime(2022, 11, 23),
) as dag:

    @task(task_id="calculate_runtime_statistics")
    def calculate_runtime_statistics(**kwargs):
        distinct_run_uuids = get_updated_run_uuids_in_data_interval(
            kwargs["dag_run"].data_interval_start, kwargs["dag_run"].data_interval_end
        )

        for run_uuid in distinct_run_uuids:
            input_run_df = DeltaTable(
                "data/pipeline_artifacts/features_pipeline/combined_features"
            ).to_pandas(
                partitions=[("run_uuid", "=", run_uuid)],
            )

            input_run_df = input_run_df.groupby(["run_uuid"]).agg(
                {"time_stamp": ["max", "min"], "d1": ["sum"], "d2": ["sum"]}
            )
            input_run_df.columns = [
                "_".join((field, str(type))) for field, type in input_run_df.columns
            ]

            input_run_df["total_runtime_ms"] = (
                input_run_df["time_stamp_max"] - input_run_df["time_stamp_min"]
            ) / 1000000
            input_run_df["run_start_time"] = pd.to_datetime(
                input_run_df["time_stamp_min"], utc=True
            ).apply(lambda val: val.isoformat(timespec="milliseconds"))
            input_run_df["run_stop_time"] = pd.to_datetime(
                input_run_df["time_stamp_max"], utc=True
            ).apply(lambda val: val.isoformat(timespec="milliseconds"))

            input_run_df["total_distance_traveled_1"] = input_run_df["d1_sum"]
            input_run_df["total_distance_traveled_2"] = input_run_df["d2_sum"]
            input_run_df.reset_index(inplace=True)

            input_run_df = input_run_df[
                [
                    "run_uuid",
                    "total_runtime_ms",
                    "run_start_time",
                    "run_stop_time",
                    "total_distance_traveled_1",
                    "total_distance_traveled_2",
                ]
            ]

            write_deltalake(
                "data/pipeline_artifacts/summary_pipeline/run_statistics",
                input_run_df,
                schema=_get_summary_output_schema(),
                mode="overwrite",
                engine="rust",
                predicate=f"run_uuid = '{run_uuid}'",
            )

    calculate_runtime_statistics_task = calculate_runtime_statistics()


def _get_summary_output_schema():
    return Schema(
        [
            Field("run_uuid", "string"),
            Field("run_start_time", "string"),
            Field("run_stop_time", "string"),
            Field("total_runtime_ms", "double"),
            Field("total_distance_traveled_1", "double"),
            Field("total_distance_traveled_2", "double"),
        ]
    )
