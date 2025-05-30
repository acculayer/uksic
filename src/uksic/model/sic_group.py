"""
SIC
"""

from dataclasses import dataclass
from uksic.model.sic_entity import SicEntity
from uksic.model.sic_class import SicClass

@dataclass
class SicGroup(SicEntity):
    """
    A data structure representing the entire UK Standard Industrial Classification
    map
    """

    classes: list[SicClass]
