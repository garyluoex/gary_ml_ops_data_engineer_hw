from airflow.models.dag import DAG
from airflow.decorators import task
from deltalake import DeltaTable, write_deltalake
import pyarrow as pa
import pandas as pd

with DAG(
    "ml_ops_data_engineer_hw",
    description="Process posiion and force sensor data.",
) as dag:

    @task(task_id="read_sensor_data_parquet")
    def read_sensor_data_parquet(parquet_file_path: str):
        df = pd.read_parquet(parquet_file_path)

        df = df.pivot(
            index=["time", "run_uuid"], columns=["field", "robot_id"], values="value"
        )
        df.columns = [
            "_".join((field, str(robot_id))) for field, robot_id in df.columns
        ]
        df = df.reset_index()
        df["run_uuid"] = df["run_uuid"].apply("{:.0f}".format)

        write_deltalake(
            "data/artifact/read_sensor_data",
            df,
            mode="overwrite",
            schema_mode="overwrite",
            partition_by=["run_uuid"],
        )
        return "Reading parquet file complete"

    @task(task_id="interpolate_sensor_data")
    def interpolate_sensor_data(delta_path: str):
        dt = DeltaTable(delta_path)
        run_uuid_df = dt.to_pandas(columns=["run_uuid"])
        distinct_run_uuids = run_uuid_df["run_uuid"].unique().tolist()

        first_parition = True
        for run_uuid in distinct_run_uuids:
            df = dt.to_pandas(partitions=[("run_uuid", "=", run_uuid)])
            print(df.info())
            df["timestamp"] = pd.DatetimeIndex(df.time).asi8
            df = df.set_index("timestamp")
            df = df.interpolate(method="index", limit_direction="forward", axis="index")
            df = df.ffill()
            df = df.bfill()

            if first_parition:  # change to check if table exists
                write_deltalake(
                    "data/artifact/interpolate_sensor_data",
                    df,
                    mode="overwrite",
                    schema_mode="overwrite",
                    partition_by=["run_uuid"],
                )
                first_parition = False
            else:
                write_deltalake(
                    "data/artifact/interpolate_sensor_data",
                    df,
                    mode="overwrite",
                    schema_mode="overwrite",
                    partition_filters=[("run_uuid", "=", run_uuid)],
                )
        return "Interpolating sensor data complete"

    @task(task_id="compute_features")
    def compute_features(delta_path: str):
        dt = DeltaTable(delta_path)
        df = dt.to_pandas()

        df["vx_1"] = (df.x_1 - df.x_1.shift(1)) / (
            df.timestamp - df.timestamp.shift(1) / 1000000
        )  # milimeters per milisecond

        write_deltalake(
            "data/artifact/data_with_features",
            df,
            mode="overwrite",
            schema_mode="overwrite",
            partition_by=["run_uuid"],
        )

        return "Computing features"

    @task(task_id="calculate_runtime_statistics")
    def calculate_runtime_statistics(delta_path: str):
        dt = DeltaTable(delta_path)
        df = dt.to_pandas()

        df = df.groupby(["run_uuid"]).agg({"timestamp": ["max", "min"]})

        write_deltalake(
            "data/artifact/run_statistics",
            df,
            mode="overwrite",
            schema_mode="overwrite",
        )

        return "Computing features"

    read_sample_sensor_data = read_sensor_data_parquet("data/sample.parquet")

    interpolate_data = interpolate_sensor_data("data/artifact/read_sensor_data")

    compute_features_task = compute_features("data/artifact/interpolate_sensor_data")

    calculate_runtime_statistics_task = calculate_runtime_statistics(
        "data/artifact/data_with_features"
    )

    read_sample_sensor_data >> interpolate_data

    interpolate_data >> compute_features_task

    compute_features_task >> calculate_runtime_statistics_task
