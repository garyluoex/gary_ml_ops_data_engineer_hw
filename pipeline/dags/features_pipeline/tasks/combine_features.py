import datetime
from typing import List
from airflow.decorators import task
from deltalake import DeltaTable, Field, Schema, write_deltalake

from features_pipeline.features.abstract_feature import Feature
from deltalake.exceptions import TableNotFoundError


@task(task_id="combine_features")
def combine_features(features: List[Feature], **kwargs):
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
        + [
            Field(
                feature.get_feature_column_name(),
                feature.get_feature_column_type(),
            )
            for feature in features
        ]
    )
    # create output delta table if not already exists
    try:
        DeltaTable("data/pipeline_artifacts/cleaning_pipeline/clean_and_reshape")
    except TableNotFoundError:
        DeltaTable.create(
            "data/pipeline_artifacts/cleaning_pipeline/clean_and_reshape",
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

    for run_uuid in distinct_run_uuids:
        df = DeltaTable(
            "data/pipeline_artifacts/cleaning_pipeline/interpolate_sensor_data"
        ).to_pandas(
            partitions=[
                ("run_uuid", "=", run_uuid),
            ],
        )

        for feature in features:
            feature_path = feature.get_feature_delta_table_path()
            feature_df = DeltaTable(feature_path).to_pandas(
                columns=["run_uuid", "time_stamp", feature.get_feature_column_name()],
                partitions=[
                    ("run_uuid", "=", run_uuid),
                ],
            )

            df = df.merge(
                feature_df, on=["run_uuid", "time_stamp"], how="left"
            )  # TODO: figure out why join doesn't work

        write_deltalake(
            "data/pipeline_artifacts/features_pipeline/combined_features",
            df,
            mode="overwrite",
            schema=schema,
            schema_mode="overwrite",
            partition_by=["run_uuid"],
            partition_filters=[("run_uuid", "=", run_uuid)],
        )
