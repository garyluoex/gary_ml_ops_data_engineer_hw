from airflow.models.dag import DAG
from airflow.decorators import task

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

    velocity_x_1 = AxisDifferentiationByTime("vx_1", "x_1")
    velocity_y_1 = AxisDifferentiationByTime("vy_1", "y_1")
    velocity_z_1 = AxisDifferentiationByTime("vz_1", "z_1")
    velocity_x_2 = AxisDifferentiationByTime("vx_2", "x_2")
    velocity_y_2 = AxisDifferentiationByTime("vy_2", "y_2")
    velocity_z_2 = AxisDifferentiationByTime("vz_2", "z_2")
    acceleration_x_1 = AxisDifferentiationByTime("ax_1", "vx_1", [velocity_x_1])
    acceleration_y_1 = AxisDifferentiationByTime("ay_1", "vy_1", [velocity_y_1])
    acceleration_z_1 = AxisDifferentiationByTime("az_1", "vz_1", [velocity_z_1])
    acceleration_x_2 = AxisDifferentiationByTime("ax_2", "vx_2", [velocity_x_2])
    acceleration_y_2 = AxisDifferentiationByTime("ay_2", "vy_2", [velocity_y_2])
    acceleration_z_2 = AxisDifferentiationByTime("az_2", "vz_2", [velocity_z_2])
    total_velocity_1 = EuclideanNorm(
        "v1", ["vx_1", "vy_1", "vz_1"], [velocity_x_1, velocity_y_1, velocity_z_1]
    )
    total_velocity_2 = EuclideanNorm(
        "v2", ["vx_2", "vy_2", "vz_2"], [velocity_x_2, velocity_y_2, velocity_z_2]
    )
    total_acceleration_1 = EuclideanNorm(
        "a1",
        ["ax_1", "ay_1", "az_1"],
        [acceleration_x_1, acceleration_y_1, acceleration_z_1],
    )
    total_acceleration_2 = EuclideanNorm(
        "a2",
        ["ax_2", "ay_2", "az_2"],
        [acceleration_x_2, acceleration_y_2, acceleration_z_2],
    )
    total_force_1 = EuclideanNorm("f1", ["fx_1", "fy_1", "fz_1"])
    total_force_2 = EuclideanNorm("f2", ["fx_2", "fy_2", "fz_2"])
    distance_1 = EuclideanNorm("d1", ["x_1", "y_1", "z_1"])
    distance_2 = EuclideanNorm("d2", ["x_2", "y_2", "z_2"])

    # There must be no cycle in the dependency graph of the tasks
    # The tasks must be ordered in tological sorted order
    features = [
        velocity_x_1,
        velocity_y_1,
        velocity_z_1,
        velocity_x_2,
        velocity_y_2,
        velocity_z_2,
        acceleration_x_1,
        acceleration_y_1,
        acceleration_z_1,
        acceleration_x_2,
        acceleration_y_2,
        acceleration_z_2,
        total_velocity_1,
        total_velocity_2,
        total_acceleration_1,
        total_acceleration_2,
        total_force_1,
        total_force_2,
        distance_1,
        distance_2,
    ]

    generate_feature_tasks = {
        feature.get_feature_name(): generate_feature.override(
            task_id=f"generate_feature_{feature.get_feature_name()}"
        )("data/pipeline_artifacts/interpolate_sensor_data", feature)
        for feature in features
    }

    combine_features_task = combine_features(
        "data/pipeline_artifacts/interpolate_sensor_data", features
    )

    for feature in features:
        generate_feature_tasks[feature.get_feature_name()] << [
            generate_feature_tasks[dependent_feature.get_feature_name()]
            for dependent_feature in feature.get_dependent_features()
        ]

    combine_features_task << [
        feature_task for feature_task in generate_feature_tasks.values()
    ]
