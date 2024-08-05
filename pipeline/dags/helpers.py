from datetime import datetime
from typing import List
from deltalake import DeltaTable, Schema
from deltalake.exceptions import TableNotFoundError


# start_date inclusice and end_date is exclusive
def get_updated_run_uuids_in_data_interval(
    start_date: datetime,
    end_date: datetime,
) -> List[str]:
    partitioned_sensor_data_dt = DeltaTable(
        "data/pipeline_artifacts/partition_pipeline/parition_sensor_data_by_run_uuid"
    )
    run_uuid_df = partitioned_sensor_data_dt.to_pandas(
        columns=["run_uuid"],
        partitions=[
            ("delivery_date", ">=", start_date.strftime("%Y%m%d")),
            ("delivery_date", "<", end_date.strftime("%Y%m%d")),
        ],
    )
    return run_uuid_df["run_uuid"].unique().tolist()
