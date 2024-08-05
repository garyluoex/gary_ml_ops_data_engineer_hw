from typing import List

import numpy as np
from pandas import DataFrame, Series
from features_pipeline.feature_builders.abstract_feature import Feature


class EuclideanNorm(Feature):
    def __init__(self, feature_name: str, axis_columns: List[str]):
        self.feature_name = feature_name
        self.axis_columns = axis_columns

    def get_feature_column_name(self) -> str:
        return self.feature_name

    def get_feature_column_type(self) -> str:
        return "double"

    def get_dependent_columns(self) -> List[str]:
        return self.axis_columns

    def compute_feature(
        self, df: DataFrame
    ) -> Series:  # Do I need ot sort this by timestamp first?
        df[self.feature_name] = np.linalg.norm(df[self.axis_columns].values, axis=1)
        return df

    def get_feature_constraints(self):
        return [
            {"value_too_large": f"{self.get_feature_column_name()} < 1000000000"},
            {
                "value_too_large_negative": f"{self.get_feature_column_name()} > -1000000000"
            },
        ]
