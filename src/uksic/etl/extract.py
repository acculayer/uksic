"""
Open downloaded payload
"""

from csv import QUOTE_NOTNULL
import json
import logging
from pathlib import Path
from pandas import DataFrame, read_excel
from uksic.etl.model import SicExtract

class Extractor:
    """
    ETL extractor using pandas data frames
    """

    def __init__(
        self,
        src_path: Path | None = None,
        dst_dir: Path | None = None,
        df: DataFrame | None = None
    ):
        self.src_path = src_path
        self.dst_dir = dst_dir
        self.df = df
        self.sic_extract = SicExtract()

        self.load_df()


    def load_df(self):
        """
        Load dataframe, either from instance variable, or from disk
        """
        if self.df is None and self.src_path:
            self.df = read_excel(io=self.src_path)


    def write_csv(self, df: DataFrame, dst_path: Path):
        """
        Write a dataframe to a local CSV
        """

        logging.info('Writing to CSV: %s', dst_path)
        df.to_csv(path_or_buf=dst_path, index=False, quoting=QUOTE_NOTNULL, na_rep=None)


    def extract(self) -> SicExtract:
        """
        Use pandas to extract rows
        """

        self.load_df()

        # Extract into a single CSV
        self.calculate_all()

        # Extract each level into separate CSVs
        self.extract_sections()
        self.extract_divisions()
        self.extract_groups()
        self.extract_classes()
        self.extract_subclasses()

        # Export extracts into data structures
        self.export()

        return self.sic_extract


    def extract_rows(self, level: str, columns: dict, filename: str) -> DataFrame:
        """
        Given column configuration, extract rows. Description column is always extracted.
        Applies text formatting. Writes rows to CSV.
        """

        if 'id' not in columns.values():
            raise ValueError('mapped id column must be specified')

        # Ensure summary (Description field in raw data) is always extracted
        columns['summary'] = 'summary'
        rows = self.df[self.df['level'] == level].rename(
            columns=columns
        )[columns.values()]


        for column in columns.values():
            rows[column] = [str(i).strip() for i in rows[column]]

        # Write to CSV
        self.write_csv(df=rows, dst_path=self.dst_dir.joinpath(filename))

        return rows


    def extract_sections(self):
        """
        Extract sections from raw dataframe
        """

        self.sic_extract.sections = self.extract_rows(
            level='Section',
            columns={'section': 'id'},
            filename='sections.csv'
        )


    def extract_divisions(self):
        """
        Extract divisions from raw dataframe
        """

        self.sic_extract.divisions = self.extract_rows(
            level='Division',
            columns={'division': 'id', 'section': 'section_id'},
            filename='divisions.csv'
        )


    def extract_groups(self):
        """
        Extract groups from raw dataframe
        """

        self.sic_extract.groups = self.extract_rows(
            level='Group',
            columns={'group': 'id', 'division': 'division_id'},
            filename='groups.csv'
        )


    def extract_classes(self):
        """
        Extract classes from raw dataframe
        """

        self.sic_extract.classes = self.extract_rows(
            level='Class',
            columns={'class': 'id', 'group': 'group_id'},
            filename='classes.csv'
        )


    def extract_subclasses(self):
        """
        Extract subclasses from raw dataframe
        """

        self.sic_extract.subclasses = self.extract_rows(
            level='Sub class',
            columns={'subclass': 'id', 'class': 'class_id'},
            filename='subclasses.csv'
        )


    def calculate_all(self):
        """
        Combines all levels into a flat dataframe.
        Renames all columns. This method should be called before separate extractors.
        """

        self.df.rename(
            inplace=True,
            columns={
                'Description': 'summary',
                'SECTION': 'section',
                'Division': 'division',
                'Group': 'group',
                'Class': 'class',
                'Sub Class': 'subclass',
                'Most disaggregated level': 'term',
                'Level headings': 'level',
            }
        )

        self.df.replace(
            to_replace='^na$',
            value='',
            regex=True,
            inplace=True
        )

        for column in self.df.columns:

            self.df[column] = [str(i).strip().capitalize() for i in self.df[column]]

        # Write to CSV
        self.write_csv(df=self.df, dst_path=self.dst_dir.joinpath('combined.csv'))


    def export(self):
        """
        Export to python dict file
        """

        # self.sic_extract.subclasses.to_json(path_or_buf=self.dst_dir.joinpath('sic.json'))

        output = []

        for _, section in self.sic_extract.sections.iterrows():
            section_id = section['id']
            # Divisions
            divisions = []
            for _, division in self.sic_extract.divisions.iterrows():
                if section_id == division['section_id']:
                    division_id = division['id']
                    # Groups
                    groups = []
                    for _, group in self.sic_extract.groups.iterrows():
                        if division_id == group['division_id']:
                            # Classes
                            group_id = group['id']
                            classes = []

                            for _, sic_class in self.sic_extract.classes.iterrows():
                                if group_id == sic_class['group_id']:
                                    class_id = sic_class['id']
                                    classes.append({
                                        'id': class_id,
                                        'summary': sic_class['summary'],
                                        'subclasses': []
                                    })

                            groups.append({
                                'id': group['id'],
                                'summary': division['summary'],
                                'classes': classes
                            })

                    divisions.append({
                        'id': division['id'],
                        'summary': division['summary'],
                        'groups': groups,
                    })

            # Sections
            item = {
                'id': section['id'],
                'summary': section['summary'],
                'divisions': divisions
            }

            output.append(item)

        print(output)

        # output = {}
        # for subclass in self.sic_extract.subclasses:
        #     output[subclass] =

        with open(file=self.dst_dir.joinpath('sic.json'), mode='w', encoding='utf8') as fh:
            fh.write(json.dumps(output, indent=2))
