import datetime
from typing import List
from airflow.decorators import task
from deltalake import DeltaTable, Field, Schema, write_deltalake

from features_pipeline.feature_builders.abstract_feature import Feature

from helpers import get_updated_run_uuids_in_data_interval


def _get_combine_features_task_output_schema(features: List[Feature]) -> Schema:
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
        + [
            Field(
                feature.get_feature_column_name(),
                feature.get_feature_column_type(),
            )
            for feature in features
        ]
    )


@task(task_id="combine_features_task")
def combine_features_task(features: List[Feature], **kwargs):
    """
    Description: Joins all configured features into one table based on interpolated data for updated runs within data interval.

    Input: Updated run_uuids within data interval, all features for the updated runs and interpolated data filtered to only the updated runs.

    Output: Combined data with interpolated table and all features for the updated runs within the data interval.

    Reprocessing: Reprocess will overwrite old run_uuid's combined data with newly reprocessed combined data for the same run_uuid.
    """
    output_schema = _get_combine_features_task_output_schema(features=features)

    updated_run_uuids = get_updated_run_uuids_in_data_interval(
        kwargs["dag_run"].data_interval_start, kwargs["dag_run"].data_interval_end
    )

    for run_uuid in updated_run_uuids:
        df = DeltaTable(
            "data/pipeline_artifacts/preprocess_pipeline/interpolated_data"
        ).to_pandas(partitions=[("run_uuid", "=", run_uuid)])

        for feature in features:
            feature_path = feature.get_feature_delta_table_path()
            feature_df = DeltaTable(feature_path).to_pandas(
                columns=["run_uuid", "time_stamp", feature.get_feature_column_name()],
                partitions=[("run_uuid", "=", run_uuid)],
            )

            df = df.merge(feature_df, on=["run_uuid", "time_stamp"], how="left")

        write_deltalake(
            "data/pipeline_artifacts/features_pipeline/combined_data",
            df,
            mode="overwrite",
            schema=output_schema,
            schema_mode="overwrite",
            partition_by=["run_uuid"],
            partition_filters=[("run_uuid", "=", run_uuid)],
        )
