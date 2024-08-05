from typing import List

from pandas import DataFrame, Series
from features_pipeline.feature_builders.abstract_feature import Feature


class AxisDifferentiation(Feature):
    def __init__(self, feature_name: str, axis_column: str):
        self.feature_name = feature_name
        self.axis_column = axis_column

    def get_feature_column_name(self) -> str:
        return self.feature_name

    def get_dependent_columns(self) -> List[str]:
        return [self.axis_column]

    def get_feature_column_type(self) -> str:
        return "double"

    def compute_feature(self, df: DataFrame) -> DataFrame:
        df.sort_values(by="time_stamp", ascending=True, inplace=True)
        df[self.feature_name] = df[self.axis_column].diff()
        return df

    def get_feature_constraints(self):
        return [
            {"value_too_large": f"{self.get_feature_column_name()} < 1000000000"},
            {
                "value_too_large_negative": f"{self.get_feature_column_name()} > -1000000000"
            },
        ]
