"""
Generic SIC Base entity inherited by other concrete entities, e.g. Section, Division, etc.
"""

from dataclasses import dataclass

@dataclass
class SicEntity:
    """
    A data structure representing the entire UK Standard Industrial Classification
    map
    """

    id: str
    summary: str
