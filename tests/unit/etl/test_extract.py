"""
Test extractor class
"""

from pathlib import Path
from pandas import DataFrame, read_csv
from pandas.testing import assert_series_equal
from pytest import raises
from uksic.etl.extract import Extractor

DATA_DIR = Path(__file__).parent.joinpath('data')
SRC_PATH = DATA_DIR.joinpath('ons').joinpath('publisheduksicsummaryofstructureworksheet.xlsx')

def test_no_id_column():
    """
    Verify that an exception is thrown if an id column is not specified
    """
    extractor = Extractor()
    with raises(ValueError):
        extractor.extract_rows(level='abc', columns={'no_id': 'abc'}, filename='test.csv')


def test_extract_files():
    """
    Test extracting expected XLSX into CSVs. Verify extracted CSVs contain the expected columns
    """

    dst_dir = DATA_DIR.joinpath('extract').joinpath('empty')

    # Delete any files before running tests
    for item in dst_dir.iterdir():
        if item.is_file() and str(item).endswith('.csv'):
            item.unlink()

    extractor = Extractor(
        src_path=SRC_PATH,
        dst_dir=dst_dir
    )

    extractor.extract()

    expected = {
        'sections': {
            'row_count': 21,
            'data': {
                'id': ['A', 'U'],
                'summary': [
                    'Agriculture, forestry and fishing',
                    'Activities of extraterritorial organisations and bodies',
                ]
            },
        },
        'divisions': {
            'columns': ['id', 'section_id', 'summary'],
            'row_count': 88,
            'data': {
                'id': ['01', '99'],
                'section_id': ['A', 'U'],
                'summary': [
                    'Crop and animal production, hunting and related service activities',
                    'Activities of extraterritorial organisations and bodies'
                ]
            },
        },
        'groups': {
            'columns': ['id', 'division_id', 'summary'],
            'row_count': 272,
            'data': {
                'id': ['011', '990'],
                'division_id': ['01', '99'],
                'summary': [
                    'Growing of non-perennial crops',
                    'Activities of extraterritorial organisations and bodies',
                ]
            },
        },
        'classes': {
            'columns': ['id', 'group_id', 'summary'],
            'row_count': 615,
            'data': {
                'id': ['0111', '9900'],
                'group_id': ['011','990'],
                'summary': [
                    'Growing of cereals (except rice), leguminous crops and oil seeds',
                    'Activities of extraterritorial organisations and bodies',
                ]
            },
        },
        'subclasses': {
            'columns': ['id', 'class_id', 'summary'],
            'row_count': 191,
            'data': {
                'id': ['01621', '93199'],
                'class_id': ['0162', '9319'],
                'summary': [
                    'Farm animal boarding and care',
                    'Other sports activities (not including activities of racehorse owners) nec',
                ]
            },
        }
    }

    for csv_name, expected_data in expected.items():
        csv_path = dst_dir.joinpath(f'{csv_name}.csv')
        df = read_csv(filepath_or_buffer=csv_path, dtype='string')
        assert list(df) == list(dict(expected_data['data']).keys())
        assert len(df) == expected_data['row_count']

        actual_series = df.iloc[0]
        expected_series =  DataFrame(data=expected_data['data'], dtype='string').iloc[0]

        assert_series_equal(expected_series, actual_series)
