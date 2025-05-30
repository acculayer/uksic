"""
Test extractor class
"""

from pathlib import Path
from pytest import raises
from uksic.etl.transform import Transformer

DATA_DIR = Path(__file__).parent.joinpath('data')
FILENAME = 'publisheduksicsummaryofstructureworksheet'
SRC_PATH_XLSX = DATA_DIR.joinpath('ons').joinpath(f'{FILENAME}.xlsx')
SRC_PATH_CSV = DATA_DIR.joinpath('ons').joinpath(f'{FILENAME}.csv')
EMPTY_DIR = DATA_DIR.joinpath('extract').joinpath('empty')

def test_transform():
    """
    Verify that an exception is thrown if an id column is not specified
    """
    
    transformer = Transformer()
    with raises(ValueError):
        extractor.extract_rows(level='abc', columns={'no_id': 'abc'}, filename='test.csv')
