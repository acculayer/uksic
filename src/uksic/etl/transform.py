"""
Transform dataframes to output structures
"""

from csv import QUOTE_NOTNULL
import json
import logging
from pandas import DataFrame
from uksic.etl.model import SicExtract

class Transformer:
    """
    ETL transformer that operates on pandas data frames
    """

    def __init__(
        self,
        df: DataFrame
    ):
        self.df = df
        self.output: dict | None = None


    def transform(self):
        """
        Transform a given dataframe into a nested dict
        """

        for _, section in self.df.sections.iterrows():
            print(section)

    # def export(self):
    #     """
    #     Export to python dict file
    #     """

    #     # self.sic_extract.subclasses.to_json(path_or_buf=self.dst_dir.joinpath('sic.json'))

    #     output = []

    #     for _, section in self.sic_extract.sections.iterrows():
    #         section_id = section['id']
    #         # Divisions
    #         divisions = []
    #         for _, division in self.sic_extract.divisions.iterrows():
    #             if section_id == division['section_id']:
    #                 division_id = division['id']
    #                 # Groups
    #                 groups = []
    #                 for _, group in self.sic_extract.groups.iterrows():
    #                     if division_id == group['division_id']:
    #                         # Classes
    #                         group_id = group['id']
    #                         classes = []

    #                         for _, sic_class in self.sic_extract.classes.iterrows():
    #                             if group_id == sic_class['group_id']:
    #                                 class_id = sic_class['id']
    #                                 classes.append({
    #                                     'id': class_id,
    #                                     'summary': sic_class['summary'],
    #                                     'subclasses': []
    #                                 })

    #                         groups.append({
    #                             'id': group['id'],
    #                             'summary': division['summary'],
    #                             'classes': classes
    #                         })

    #                 divisions.append({
    #                     'id': division['id'],
    #                     'summary': division['summary'],
    #                     'groups': groups,
    #                 })

    #         # Sections
    #         item = {
    #             'id': section['id'],
    #             'summary': section['summary'],
    #             'divisions': divisions
    #         }

    #         output.append(item)

    #     print(output)

    #     # output = {}
    #     # for subclass in self.sic_extract.subclasses:
    #     #     output[subclass] =

    #     with open(file=self.dst_dir.joinpath('sic.json'), mode='w', encoding='utf8') as fh:
    #         fh.write(json.dumps(output, indent=2))
