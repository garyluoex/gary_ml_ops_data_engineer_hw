from typing import List
from airflow.decorators import task
import numpy as np
import pandas as pd
from pandas import DataFrame
from deltalake import DeltaTable, Field, Schema, write_deltalake

from helpers import get_updated_run_uuids_in_data_interval


def _get_pivot_task_output_schema():
    return Schema(
        [
            Field("run_uuid", "string"),
            Field("time", "string"),
            Field("x_1", "double"),
            Field("y_1", "double"),
            Field("z_1", "double"),
            Field("x_2", "double"),
            Field("y_2", "double"),
            Field("z_2", "double"),
            Field("fx_1", "double"),
            Field("fy_1", "double"),
            Field("fz_1", "double"),
            Field("fx_2", "double"),
            Field("fy_2", "double"),
            Field("fz_2", "double"),
        ]
    )


@task(task_id="pivot_task")
def pivot_task(**kwargs):
    """
    Description: Pivots `field`` and `robot_id` values into column wide format.

    Input: Partitioned data filtered to only runs that have been updated within the dag run data interval.

    Output: Pivoted data paritioned by 'run_uuid'.

    Reprocessing: Reprocess will overwrite old run data with newly reprocessed run data.
    """
    outputSchema = _get_pivot_task_output_schema()

    updated_run_uuids = get_updated_run_uuids_in_data_interval(
        kwargs["dag_run"].data_interval_start, kwargs["dag_run"].data_interval_end
    )

    partitioned_sensor_data_dt = DeltaTable(
        "data/pipeline_artifacts/preprocess_pipeline/paritioned_data"
    )
    for run_uuid in updated_run_uuids:
        run_df = partitioned_sensor_data_dt.to_pandas(
            partitions=[
                ("run_uuid", "=", run_uuid),
            ]
        )

        # pivot 'field' and 'robot_id' values into columns
        pivoted_df = _pivot_and_rename_columns(
            run_df,
            index=["time", "run_uuid"],
            columns=["field", "robot_id"],
            value="value",
        )

        # if any missing columns, fill with null
        complete_df = _insert_missing_columns(pivoted_df, outputSchema)

        # write output data to delta table
        write_deltalake(
            "data/pipeline_artifacts/preprocess_pipeline/pivoted_data",
            complete_df,
            schema=outputSchema,
            mode="overwrite",
            partition_by=["run_uuid"],
            partition_filters=[("run_uuid", "=", run_uuid)],
        )


def _pivot_and_rename_columns(
    df: DataFrame, index: List[str], columns: List[str], value: str
) -> DataFrame:
    pivot_df = df.pivot(index=index, columns=columns, values=value)
    pivot_df.columns = [
        "_".join((field, str(robot_id))) for field, robot_id in pivot_df.columns
    ]
    pivot_df.reset_index(inplace=True)
    return pivot_df


def _insert_missing_columns(df: pd.DataFrame, outputSchema: Schema) -> DataFrame:
    table_column_names = [
        field.name
        for field in outputSchema.fields
        if field.name not in ["run_uuid", "time"]
    ]
    data_column_names = df.columns
    for missing_col_name in [
        col for col in table_column_names if col not in data_column_names
    ]:
        df[missing_col_name] = np.nan
    return df
