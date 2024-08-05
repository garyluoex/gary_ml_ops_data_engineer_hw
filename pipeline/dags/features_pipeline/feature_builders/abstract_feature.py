from abc import ABC, abstractmethod
from typing import List

from pandas import DataFrame


class Feature(ABC):

    @abstractmethod
    def get_feature_column_name(self) -> str:
        pass

    @abstractmethod
    def get_feature_column_type(self) -> str:
        pass

    @abstractmethod
    def get_dependent_columns(self) -> List[str]:
        pass

    @abstractmethod
    def compute_feature(
        self,
    ) -> DataFrame:  # TODO: make sure a completely empty column works also
        pass

    @abstractmethod
    def get_feature_constraints(self):
        pass

    def get_feature_delta_table_path(self) -> str:
        return f"data/pipeline_artifacts/features_pipeline/features/{self.get_feature_column_name()}"
