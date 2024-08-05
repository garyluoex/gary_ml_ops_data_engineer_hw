import datetime
from typing import Dict
from airflow.decorators import task
from deltalake import DeltaTable, Field, Schema, write_deltalake

from features_pipeline.features.abstract_feature import Feature
from deltalake.exceptions import TableNotFoundError


# one caveat of this approach with run_uuid is that it could get reprocessed multiple times when backfilling
# This task generates a feature from the given feature
@task(task_id="generate_feature")
def generate_feature(feature: Feature, features_map: Dict[str, Feature], **kwargs):
    feature_name = feature.get_feature_column_name()
    feature_type = feature.get_feature_column_type()
    dependent_columns = feature.get_dependent_columns()
    try:
        DeltaTable(feature.get_feature_delta_table_path())
    except TableNotFoundError:
        schema = Schema(
            [
                Field("run_uuid", "string"),
                Field("time_stamp", "long"),
                Field(feature_name, feature_type),
            ]
        )
        DeltaTable.create(
            feature.get_feature_delta_table_path(),
            schema=schema,
            mode="error",
            partition_by=["run_uuid"],
        )

    data_interval_start = kwargs["dag_run"].data_interval_start
    data_interval_end = kwargs["dag_run"].data_interval_end
    assert data_interval_end - data_interval_start == datetime.timedelta(
        days=1
    ), "This pipeline is desinged to process data one day at a time."

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

    # generate features for each run_uuid
    for run_uuid in distinct_run_uuids:
        input_dt = DeltaTable(
            "data/pipeline_artifacts/cleaning_pipeline/interpolate_sensor_data"
        )
        run_df = input_dt.to_pandas(
            columns=["run_uuid", "time_stamp"]
            + [column for column in dependent_columns if column not in features_map],
            partitions=[
                ("run_uuid", "=", run_uuid),
            ],
        )

        for dependent_feature in [
            features_map[column]
            for column in dependent_columns
            if column in features_map
        ]:
            feature_dt = DeltaTable(dependent_feature.get_feature_delta_table_path())
            dependent_feature_df = feature_dt.to_pandas()
            run_df = run_df.merge(
                dependent_feature_df, on=["run_uuid", "time_stamp"], how="left"
            )

        run_df[feature_name] = feature.compute_feature(run_df)

        run_df = run_df[["run_uuid", "time_stamp", feature_name]]

        write_deltalake(
            feature.get_feature_delta_table_path(),
            run_df,
            mode="overwrite",
            schema_mode="overwrite",
            partition_filters=[("run_uuid", "=", run_uuid)],
        )
