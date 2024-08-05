import datetime
from airflow.models.dag import DAG
from preprocess_pipeline.tasks.pivot_task import (
    pivot_task,
)
from preprocess_pipeline.tasks.interpolate_task import interpolate_task
from preprocess_pipeline.tasks.partition_task import (
    partition_task,
)


with DAG(
    "preprocess_pipeline",
    schedule=datetime.timedelta(days=1),
    start_date=datetime.datetime(2022, 11, 23),
    end_date=datetime.datetime(2022, 11, 23),
) as dag:
    """
    Description: Paritions data by delivery_date and run_uuid, pivots the data to wide format, and interpolates mismatched timestamp values.

    Input: Raw sensor data paritioned by 'deliver_date'. For now we only have 'data/source/delivery_date=20221123/sample.parquet'.

    Output: Processed sensor data ready for use to generate features.

    Schedule: Runs daily at 00:00 UTC to process all sensor data from the past day. Configured to run for 2022-11-23 only at the moment since we only have 'sample.parquet'.

    Reprocessing: pivot_task and intepolate_task is idempotent, partition_task is not idempotent and will duplicate data if ran multiple times.
    """

    # create tasks
    partition_task_instance = partition_task()
    pivot_task_instance = pivot_task()
    interpolate_task_instance = interpolate_task()

    # setup dependencies
    partition_task_instance >> pivot_task_instance
    pivot_task_instance >> interpolate_task_instance
