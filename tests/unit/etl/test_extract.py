"""
Test extractor class
"""

from pathlib import Path
from pytest import raises
from uksic.etl.extract import Extractor
from tests.unit.etl.utils import (
    assert_csvs_are_correct,
    assert_combined_csv_is_correct
)

DATA_DIR = Path(__file__).parent.joinpath('data')
FILENAME = 'publisheduksicsummaryofstructureworksheet'
SRC_PATH_XLSX = DATA_DIR.joinpath('ons').joinpath(f'{FILENAME}.xlsx')
SRC_PATH_CSV = DATA_DIR.joinpath('ons').joinpath(f'{FILENAME}.csv')
EMPTY_DIR = DATA_DIR.joinpath('extract').joinpath('empty')

def test_no_id_column():
    """
    Verify that an exception is thrown if an id column is not specified
    """
    extractor = Extractor()
    with raises(ValueError):
        extractor.extract_rows(level='abc', columns={'no_id': 'abc'}, filename='test.csv')


def test_extract_csvs():
    """
    Test extracting expected XLSX into CSVs. Verify extracted CSVs contain the expected columns
    """

    # Delete any files before running tests
    for item in EMPTY_DIR.iterdir():
        if item.is_file() and str(item).endswith('.csv'):
            item.unlink()

    extractor = Extractor(
        src_path=SRC_PATH_XLSX,
        dst_dir=EMPTY_DIR
    )

    extractor.extract()

    assert_csvs_are_correct(data_dir=EMPTY_DIR)


def test_extract_combined_csv():
    """
    Verify that single CSV containing all items is correctly extracted.
    If file exists, deletes file before running test
    """

    dst_file = EMPTY_DIR.joinpath('combined.csv')
    dst_file.unlink(missing_ok=True)

    extractor = Extractor(
        src_path=SRC_PATH_XLSX,
        dst_dir=EMPTY_DIR
    )

    extractor.extract()

    assert_combined_csv_is_correct(actual_file=SRC_PATH_CSV, expected_file=dst_file)
