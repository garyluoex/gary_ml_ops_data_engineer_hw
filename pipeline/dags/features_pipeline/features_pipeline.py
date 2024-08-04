from typing import List
from airflow.models.dag import DAG
from airflow.decorators import task

from features_pipeline.features.abstract_feature import Feature
from features_pipeline.features.euclidean_norm import EuclideanNorm
from features_pipeline.tasks.generate_feature import generate_feature
from features_pipeline.tasks.combine_features import combine_features
from features_pipeline.features.axis_differentiation_by_time import (
    AxisDifferentiationByTime,
)


with DAG(
    "features_pipeline",
    description="Compute features on cleaned sensor data.",
) as dag:

    # Create the features we would like to generate and spcify how to compute it
    # Becareful not to introuduce cyclic dependencies between features
    # TODO: validate features have unique names
    # TODO: validate feature output have same shape

    features: List[Feature] = [
        AxisDifferentiationByTime("vx_1", "x_1"),
        AxisDifferentiationByTime("vy_1", "y_1"),
        AxisDifferentiationByTime("vz_1", "z_1"),
        AxisDifferentiationByTime("vx_2", "x_2"),
        AxisDifferentiationByTime("vy_2", "y_2"),
        AxisDifferentiationByTime("vz_2", "z_2"),
        AxisDifferentiationByTime("ax_1", "vx_1"),
        AxisDifferentiationByTime("ay_1", "vy_1"),
        AxisDifferentiationByTime("az_1", "vz_1"),
        AxisDifferentiationByTime("ax_2", "vx_2"),
        AxisDifferentiationByTime("ay_2", "vy_2"),
        AxisDifferentiationByTime("az_2", "vz_2"),
        EuclideanNorm("v1", ["vx_1", "vy_1", "vz_1"]),
        EuclideanNorm("v2", ["vx_2", "vy_2", "vz_2"]),
        EuclideanNorm("a1", ["ax_1", "ay_1", "az_1"]),
        EuclideanNorm("a2", ["ax_2", "ay_2", "az_2"]),
        EuclideanNorm("f1", ["fx_1", "fy_1", "fz_1"]),
        EuclideanNorm("f2", ["fx_2", "fy_2", "fz_2"]),
    ]
    # TODO: need to add distance features
    features_map = {feature.get_feature_column_name(): feature for feature in features}

    # translate the features we would like to generate into tasks
    generate_feature_tasks = {
        feature.get_feature_column_name(): generate_feature.override(
            task_id=f"generate_{feature.get_feature_column_name()}"
        )(
            "data/pipeline_artifacts/cleaning_pipeline/interpolate_sensor_data",
            feature,
            features_map,
        )
        for feature in features
    }

    # make sure the feature name don't have any duplicates causing feature to get dropped
    assert len(generate_feature_tasks) == len(
        features
    ), "Duplicate feature names are not allowed"

    # setup task dependencies between each feature
    for feature in features:
        generate_feature_tasks[feature.get_feature_column_name()] << [
            generate_feature_tasks[dependent_feature_name]
            for dependent_feature_name in feature.get_dependent_columns()
            if dependent_feature_name in generate_feature_tasks
        ]

    # create task to combine all individual feature columns into one table
    combine_features_task = combine_features(
        "data/pipeline_artifacts/cleaning_pipeline/interpolate_sensor_data", features
    )

    # combine task depends on all feature task completions
    combine_features_task << [
        feature_task for feature_task in generate_feature_tasks.values()
    ]
