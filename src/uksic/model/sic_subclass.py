"""
SIC Subclass model
"""

from dataclasses import dataclass
from uksic.model.sic_entity import SicEntity

@dataclass
class SicSubclass(SicEntity):
    """
    A data structure representing the entire UK Standard Industrial Classification
    map
    """
