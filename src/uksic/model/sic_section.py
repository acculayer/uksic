"""
SIC
"""

from dataclasses import dataclass
from uksic.model.sic_entity import SicEntity
from uksic.model.sic_division import SicDivision

@dataclass
class SicSection(SicEntity):
    """
    A data structure representing the entire UK Standard Industrial Classification
    map
    """

    divisions: list[SicDivision]
