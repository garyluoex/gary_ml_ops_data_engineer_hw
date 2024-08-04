from abc import ABC, abstractmethod
from typing import List

from pandas import DataFrame, Series


class Feature(ABC):

    @abstractmethod
    def get_feature_name(self) -> str:
        pass

    @abstractmethod
    def get_dependent_columns(self) -> List[str]:
        pass

    @abstractmethod
    def get_dependent_features(self) -> List["Feature"]:
        pass

    @abstractmethod
    def compute_feature(self) -> DataFrame:
        pass

    @abstractmethod
    def get_feature_description(self):
        pass

    @abstractmethod
    def get_feature_constraints(self):
        pass

    def get_feature_delta_table_path(self) -> str:
        return f"data/pipeline_artifacts/features_pipeline/{self.get_feature_name()}"
