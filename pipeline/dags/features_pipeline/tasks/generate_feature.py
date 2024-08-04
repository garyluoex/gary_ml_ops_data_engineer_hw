from typing import Dict
from airflow.decorators import task
from deltalake import DeltaTable, Field, Schema, write_deltalake

from features_pipeline.features.abstract_feature import Feature
from deltalake.exceptions import TableNotFoundError


@task(task_id="generate_feature")
def generate_feature(
    delta_path: str, feature: Feature, features_map: Dict[str, Feature]
):
    feature_name = feature.get_feature_column_name()
    feature_type = feature.get_feature_column_type()
    dependent_columns = feature.get_dependent_columns()

    dt = DeltaTable(delta_path)
    df = dt.to_pandas(
        columns=["run_uuid", "time_stamp"]
        + [column for column in dependent_columns if column not in features_map]
    )

    for dependent_feature in [
        features_map[column] for column in dependent_columns if column in features_map
    ]:
        feature_dt = DeltaTable(dependent_feature.get_feature_delta_table_path())
        dependent_feature_df = feature_dt.to_pandas()
        df = df.merge(dependent_feature_df, on=["run_uuid", "time_stamp"], how="left")

    df[feature_name] = feature.compute_feature(df)

    df = df[["run_uuid", "time_stamp", feature_name]]

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

    write_deltalake(
        feature.get_feature_delta_table_path(),
        df,
        mode="overwrite",
        schema_mode="overwrite",
        partition_by=["run_uuid"],
    )

    return f"Computed feature {feature_name} "
