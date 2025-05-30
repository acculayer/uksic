"""
SIC
"""

from dataclasses import dataclass
from uksic.model.sic_entity import SicEntity
from uksic.model.sic_subclass import SicSubclass

@dataclass
class SicClass(SicEntity):
    """
    A data structure representing the entire UK Standard Industrial Classification
    map
    """

    subclasses: list[SicSubclass]
