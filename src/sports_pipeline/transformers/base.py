"""Base transformer interface."""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

from sports_pipeline.utils.logging import get_logger


class BaseTransformer(ABC):
    """Base class for all data transformers (Bronze -> Silver)."""

    def __init__(self) -> None:
        self.log = get_logger(self.__class__.__name__)

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform a bronze DataFrame into silver-layer format."""
        ...
