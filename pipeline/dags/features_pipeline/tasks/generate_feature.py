from airflow.decorators import task
from deltalake import DeltaTable, write_deltalake

from features_pipeline.features.abstract_feature import Feature


@task(task_id="generate_feature")
def generate_feature(delta_path: str, feature: Feature):
    feature_name = feature.get_feature_name()
    dependent_features = feature.get_dependent_features()
    dependent_columns = feature.get_dependent_columns()

    dt = DeltaTable(delta_path)
    df = dt.to_pandas(
        columns=["run_uuid", "timestamp"]
        + [
            column
            for column in dependent_columns
            if column
            not in [
                feature.get_feature_name()
                for feature in feature.get_dependent_features()
            ]
        ]
    )

    for dependent_feature in dependent_features:
        feature_dt = DeltaTable(dependent_feature.get_feature_delta_table_path())
        dependent_feature_df = feature_dt.to_pandas()
        df = df.merge(dependent_feature_df, on=["run_uuid", "timestamp"], how="left")

    df[feature_name] = feature.compute_feature(df)

    df = df[["run_uuid", "timestamp", feature_name]]

    write_deltalake(
        feature.get_feature_delta_table_path(),
        df,
        mode="overwrite",
        schema_mode="overwrite",
        partition_by=["run_uuid"],
    )

    return f"Computed feature {feature_name} "
