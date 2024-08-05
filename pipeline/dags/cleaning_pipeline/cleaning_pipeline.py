import datetime
from airflow.models.dag import DAG
from cleaning_pipeline.tasks.clean_and_reshape import (
    clean_and_reshape,
)
from cleaning_pipeline.tasks.interpolate_sensor_data import interpolate_sensor_data
from cleaning_pipeline.tasks.partition_sensor_data import (
    parition_sensor_data,
)

with DAG(
    "cleaning_pipeline",
    description="Clean, format and interpolate sensor data.",
    schedule=datetime.timedelta(days=1),
    start_date=datetime.datetime(2022, 11, 23),
    end_date=datetime.datetime(2022, 11, 23),
) as dag:
    # create tasks
    parition_sensor_data = parition_sensor_data()
    clean_and_reshape_task = clean_and_reshape()
    interpolate_task = interpolate_sensor_data()

    # setup dependencies
    parition_sensor_data >> clean_and_reshape_task
    clean_and_reshape_task >> interpolate_task
