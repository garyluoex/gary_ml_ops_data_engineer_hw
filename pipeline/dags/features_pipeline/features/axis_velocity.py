from typing import List

from pandas import DataFrame, Series
from features_pipeline.features.abstract_feature import Feature


class AxisVelocity(Feature):
    def __init__(self, axis_position_column: str):
        self.axis_position_column = axis_position_column

    def get_feature_name(self) -> str:
        return "v" + self.axis_position_column

    def get_dependent_columns(self) -> List[str]:
        return [self.axis_position_column]

    def get_dependent_features(self) -> List["Feature"]:
        return []

    def compute_feature(
        self, df: DataFrame
    ) -> Series:  # Do I need ot sort this by timestamp first?
        return df[self.axis_position_column].diff() / df["timestamp"].diff() / 1000000

    def get_feature_description(self):
        return """
        Calcuates the velocity along a single axis given the position of the axis.      
        """

    def get_feature_constraints(self):
        return [
            {"velocity_too_large": f"{self.get_feature_name()} < 1000000000"},
            {"velocity_too_large_negative": f"{self.get_feature_name()} > -1000000000"},
        ]
