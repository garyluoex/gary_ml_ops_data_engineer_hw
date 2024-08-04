from typing import List
from airflow.decorators import task
from deltalake import DeltaTable, write_deltalake

from features_pipeline.features.abstract_feature import Feature


@task(task_id="combine_features")
def combine_features(delta_path: str, features: List[Feature]):
    dt = DeltaTable(delta_path)
    df = dt.to_pandas()

    for feature in features:
        feature_name = feature.get_feature_name()
        feature_path = feature.get_feature_delta_table_path()
        feature_df = DeltaTable(feature_path).to_pandas()
        df = df.merge(feature_df, on=["run_uuid", "timestamp"], how="left")

    write_deltalake(
        "data/pipeline_artifacts/features_pipeline/combined_features",
        df,
        mode="overwrite",
        schema_mode="overwrite",
        partition_by=["run_uuid"],
    )

    return f"Combined all {len(features)} features"
