import datetime
from airflow.models.dag import DAG
from airflow.decorators import task
from deltalake import DeltaTable, Field, Schema, write_deltalake
import pandas as pd

from helpers import get_updated_run_uuids_in_data_interval


with DAG(
    "summary_pipeline",
    schedule=datetime.timedelta(days=1),
    start_date=datetime.datetime(2022, 11, 23),
    end_date=datetime.datetime(2022, 11, 23),
) as dag:
    """
    Description: Generates run statistics data.

    Input: Updated run_uuids within data interval, combined data for updated runs within the data interval.

    Output: Run statistics for updated runs within the data interval.

    Schedule: Runs daily at 00:00 UTC to process all interpolated data filtered to only the runs with new records ingested within the dag run data interval.

    Reprocessing: Reprocess will overwrite old run_uuid's statistics with newly reprocessed statistics for the same run_uuid.
    """

    def _get_run_statistics_task_output_schema():
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

    @task(task_id="run_statistics_task")
    def run_statistics_task(**kwargs):
        """
        Description: Computes aggregated statistics for each run with new record ingested within the dag run data interval.

        Input: Updated run_uuids within data interval, combined data filtered to updated runs within the data interval.

        Output: Run statistics for updated runs within the data interval.

        Reprocessing: Reprocess will overwrite old run_uuid's statistics with newly reprocessed statistics for the same run_uuid.
        """
        distinct_run_uuids = get_updated_run_uuids_in_data_interval(
            kwargs["dag_run"].data_interval_start, kwargs["dag_run"].data_interval_end
        )

        for run_uuid in distinct_run_uuids:
            input_run_df = DeltaTable(
                "data/pipeline_artifacts/features_pipeline/combined_data"
            ).to_pandas(
                partitions=[("run_uuid", "=", run_uuid)],
            )

            # group by run to compute aggregated statistics
            input_run_df = input_run_df.groupby(["run_uuid"]).agg(
                {"time_stamp": ["max", "min"], "d1": ["sum"], "d2": ["sum"]}
            )
            input_run_df.columns = [
                "_".join((field, str(type))) for field, type in input_run_df.columns
            ]

            # compute output statistics columns
            input_run_df["total_runtime"] = (
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

            # select only the columns we want to output
            input_run_df = input_run_df[
                [
                    "run_uuid",
                    "total_runtime",
                    "run_start_time",
                    "run_stop_time",
                    "total_distance_traveled_1",
                    "total_distance_traveled_2",
                ]
            ]

            write_deltalake(
                "data/pipeline_artifacts/summary_pipeline/statistics_data",
                input_run_df,
                schema=_get_run_statistics_task_output_schema(),
                mode="overwrite",
                engine="rust",
                predicate=f"run_uuid = '{run_uuid}'",
            )

    calculate_runtime_statistics_task = run_statistics_task()
