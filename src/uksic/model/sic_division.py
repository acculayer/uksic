"""
SIC
"""

from dataclasses import dataclass
from uksic.model.sic_entity import SicEntity
from uksic.model.sic_group import SicGroup

@dataclass
class SicDivision(SicEntity):
    """
    SIC Division
    """

    groups: list[SicGroup]
