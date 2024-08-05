import datetime
from airflow.models.dag import DAG
from airflow.decorators import task
from deltalake import DeltaTable, Field, Schema, write_deltalake
from deltalake.exceptions import TableNotFoundError
import pandas as pd

with DAG(
    "summary_pipeline",
    description="Create summaries using cleaned sensor data with features.",
    schedule=datetime.timedelta(days=1),
    start_date=datetime.datetime(2022, 11, 23),
    end_date=datetime.datetime(2022, 11, 23),
) as dag:

    @task(task_id="calculate_runtime_statistics")
    def calculate_runtime_statistics(**kwargs):
        # create output delta table if not already exists
        try:
            DeltaTable("data/pipeline_artifacts/summary_pipeline/run_statistics")
        except TableNotFoundError:
            schema = Schema(
                [
                    Field("run_uuid", "string"),
                    Field("run_start_time", "string"),
                    Field("run_stop_time", "string"),
                    Field("total_runtime_ms", "string"),
                    Field("total_distance_traveled_1", "double"),
                    Field("total_distance_traveled_2", "double"),
                ]
            )
            DeltaTable.create(
                "data/pipeline_artifacts/summary_pipeline/run_statistics",
                schema=schema,
                mode="error",
            )

        data_interval_start = kwargs["dag_run"].data_interval_start
        data_interval_end = kwargs["dag_run"].data_interval_end
        assert data_interval_end - data_interval_start == datetime.timedelta(
            days=1
        ), "This task is desinged to process data one day at a time."

        # identify run_uuid that received data during the current data interval
        run_uuid_dt = DeltaTable(
            "data/pipeline_artifacts/partition_pipeline/parition_sensor_data_by_run_uuid"
        )
        run_uuid_df = run_uuid_dt.to_pandas(
            columns=["run_uuid"],
            partitions=[
                ("delivery_date", "=", data_interval_start.strftime("%Y%m%d"))
            ],  # TODO: update this to use data_interval_end in between to allow task handle larger time intervals
        )
        distinct_run_uuids = run_uuid_df["run_uuid"].unique().tolist()

        for run_uuid in distinct_run_uuids:
            dt = DeltaTable(
                "data/pipeline_artifacts/features_pipeline/combined_features"
            )
            df = dt.to_pandas(
                partitions=[
                    ("run_uuid", "=", run_uuid),
                ],
            )

            df = df.groupby(["run_uuid"]).agg(
                {"time_stamp": ["max", "min"], "d1": ["sum"], "d2": ["sum"]}
            )
            df.columns = ["_".join((field, str(type))) for field, type in df.columns]

            df["total_runtime_ms"] = (
                df["time_stamp_max"] - df["time_stamp_min"]
            ) / 1000000
            df["run_start_time"] = pd.to_datetime(df["time_stamp_min"], utc=True)
            df["run_stop_time"] = pd.to_datetime(df["time_stamp_max"], utc=True)
            df["total_distance_traveled_1"] = df["d1_sum"]
            df["total_distance_traveled_2"] = df["d2_sum"]
            df.reset_index(inplace=True)

            df = df[
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
                df,
                mode="overwrite",
                engine="rust",
                predicate=f"run_uuid = '{run_uuid}'",
            )

    calculate_runtime_statistics_task = calculate_runtime_statistics()
