from airflow.decorators import task
from deltalake import DeltaTable, write_deltalake
import pandas as pd


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
                "data/pipeline_artifacts/interpolate_sensor_data",
                df,
                mode="overwrite",
                schema_mode="overwrite",
                partition_by=["run_uuid"],
            )
            first_parition = False
        else:
            write_deltalake(
                "data/pipeline_artifacts/interpolate_sensor_data",
                df,
                mode="overwrite",
                schema_mode="overwrite",
                partition_filters=[("run_uuid", "=", run_uuid)],
            )
    return "Interpolating sensor data complete"
