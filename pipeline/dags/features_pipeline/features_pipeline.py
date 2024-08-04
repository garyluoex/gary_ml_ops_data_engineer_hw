from airflow.models.dag import DAG
from airflow.decorators import task

from features_pipeline.tasks.generate_feature import generate_feature
from features_pipeline.tasks.combine_features import combine_features
from features_pipeline.features.axis_velocity import AxisVelocity


with DAG(
    "features_pipeline",
    description="Compute features on cleaned sensor data.",
) as dag:

    # There must be no cycle in the dependency graph of the tasks
    # The tasks must be ordered in tological sorted order
    features = [
        AxisVelocity("x_1"),
        AxisVelocity("y_1"),
        AxisVelocity("z_1"),
        AxisVelocity("x_2"),
        AxisVelocity("y_2"),
        AxisVelocity("z_2"),
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
