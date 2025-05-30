"""
Models used in the ETL process
"""

from dataclasses import dataclass
from pandas import DataFrame

@dataclass
class SicExtract:
    """
    An ETL Extract representation of ONS SIC data.
    Used to extract relational data as CSVs
    """

    sections: DataFrame | None = None
    divisions: DataFrame | None = None
    groups: DataFrame | None = None
    classes: DataFrame | None = None
    subclasses: DataFrame | None = None
