from typing import Dict, List
from airflow.decorators import task
from deltalake import DeltaTable, Field, Schema, write_deltalake
from pandas import DataFrame

from features_pipeline.features.abstract_feature import Feature

from helpers import get_updated_run_uuids_in_data_interval


# one caveat of this approach with run_uuid is that it could get reprocessed multiple times when backfilling
# This task generates a feature from the given feature
@task(task_id="generate_feature")
def generate_feature(feature: Feature, features_map: Dict[str, Feature], **kwargs):

    updated_run_uuids = get_updated_run_uuids_in_data_interval(
        kwargs["dag_run"].data_interval_start, kwargs["dag_run"].data_interval_end
    )

    # generate features for each run_uuid
    feature_name = feature.get_feature_column_name()
    dependent_columns = feature.get_dependent_columns()
    for run_uuid in updated_run_uuids:
        input_interpolated_dt = DeltaTable(
            "data/pipeline_artifacts/cleaning_pipeline/interpolate_sensor_data"
        )

        # get inteporlated data for the run_uuid
        input_run_df = input_interpolated_dt.to_pandas(
            columns=["run_uuid", "time_stamp"]
            + [column for column in dependent_columns if column not in features_map],
            partitions=[
                ("run_uuid", "=", run_uuid),
            ],
        )

        # merge columns from features into interpolated data for use
        merged_features_df = _append_columns_from_dependent_features(
            input_run_df, run_uuid, dependent_columns, features_map
        )

        # generate the feature
        merged_features_df = feature.compute_feature(merged_features_df)

        # take only the neccessary columns
        merged_features_df = merged_features_df[
            ["run_uuid", "time_stamp", feature_name]
        ]

        write_deltalake(
            feature.get_feature_delta_table_path(),
            merged_features_df,
            mode="overwrite",
            schema=_get_generate_feature_output_schema(feature),
            partition_by=["run_uuid"],
            partition_filters=[("run_uuid", "=", run_uuid)],
        )


def _append_columns_from_dependent_features(
    df: DataFrame,
    run_uuid: str,
    dependent_columns: List[str],
    features_map: Dict[str, Feature],
) -> DataFrame:
    for dependent_feature in [
        feature for (name, feature) in features_map.items() if name in dependent_columns
    ]:
        feature_dt = DeltaTable(dependent_feature.get_feature_delta_table_path())
        dependent_feature_df = feature_dt.to_pandas(
            partitions=[
                ("run_uuid", "=", run_uuid),
            ],
        )
        df = df.merge(dependent_feature_df, on=["run_uuid", "time_stamp"], how="left")
    return df


def _get_generate_feature_output_schema(feature: Feature) -> Schema:
    return Schema(
        [
            Field("run_uuid", "string"),
            Field("time_stamp", "long"),
            Field(feature.get_feature_column_name(), feature.get_feature_column_type()),
        ]
    )
