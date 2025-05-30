"""
SIC
"""

from dataclasses import dataclass
from uksic.model.sic_section import SicSection

@dataclass
class Sic:
    """
    A data structure representing the entire UK Standard Industrial Classification
    map
    """

    sections: list[SicSection] | None

    def get_dict():
        """
        """
