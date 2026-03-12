"""Base extractor interface."""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

from sports_pipeline.utils.logging import get_logger


class BaseExtractor(ABC):
    """Base class for all data extractors."""

    def __init__(self) -> None:
        self.log = get_logger(self.__class__.__name__)

    @abstractmethod
    def extract(self, **kwargs) -> pd.DataFrame:
        """Extract data and return as a DataFrame."""
        ...
