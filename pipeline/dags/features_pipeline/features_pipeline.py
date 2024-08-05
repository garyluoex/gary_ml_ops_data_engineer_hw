import datetime
from typing import List
from airflow.models.dag import DAG

from features_pipeline.feature_builders.axis_differentiation import AxisDifferentiation
from features_pipeline.feature_builders.abstract_feature import Feature
from features_pipeline.feature_builders.euclidean_norm import EuclideanNorm
from dags.features_pipeline.tasks.generate_feature_task import generate_feature_task
from dags.features_pipeline.tasks.combine_features_task import combine_features_task
from features_pipeline.feature_builders.axis_differentiation_by_time import (
    AxisDifferentiationByTime,
)


with DAG(
    "features_pipeline",
    schedule=datetime.timedelta(days=1),
    start_date=datetime.datetime(2022, 11, 23),
    end_date=datetime.datetime(2022, 11, 23),
) as dag:
    """
    Description: Generates a task for each feature we want to compute, compute the feature and combing feature with together with interpolated data.

    Input: Interpolated data for runs with updated data within the dag run data interval.

    Output: Combined data with interpolated table and all features for the updated runs within the data interval.

    Schedule: Runs daily at 00:00 UTC to process all interpolated data filtered to only the runs with new records ingested within the dag run data interval.

    Reprocessing: Reprocessing will overwrite old run_uuid's features and combined data with newly reprocessed features and combined data for the same run_uuid.
    """

    # Becareful not to introuduce cyclic dependencies between features
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
        AxisDifferentiation("dx_1", "x_1"),
        AxisDifferentiation("dy_1", "y_1"),
        AxisDifferentiation("dz_1", "z_1"),
        AxisDifferentiation("dx_2", "x_2"),
        AxisDifferentiation("dy_2", "y_2"),
        AxisDifferentiation("dz_2", "z_2"),
        EuclideanNorm("d1", ["dx_1", "dy_1", "dz_1"]),
        EuclideanNorm("d2", ["dx_2", "dy_2", "dz_2"]),
    ]

    # translate the features we would like to generate into tasks
    features_map = {feature.get_feature_column_name(): feature for feature in features}
    feature_tasks_map = {
        feature.get_feature_column_name(): generate_feature_task.override(
            task_id=f"generate_{feature.get_feature_column_name()}"
        )(feature, features_map)
        for feature in features
    }

    # make sure the feature name don't have any duplicates causing feature to get dropped
    assert len(feature_tasks_map) == len(
        features
    ), "Duplicate feature names are not allowed"

    # setup task dependencies between each feature
    for feature in features:
        feature_tasks_map[feature.get_feature_column_name()] << [
            feature_tasks_map[dependent_feature_name]
            for dependent_feature_name in feature.get_dependent_columns()
            if dependent_feature_name in feature_tasks_map
        ]

    # create task to combine all individual feature columns into one table
    combine_features_task = combine_features_task(features)

    # combine task depends on all feature task completions
    combine_features_task << [
        feature_task for feature_task in feature_tasks_map.values()
    ]
