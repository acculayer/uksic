"""
Retrieve remote payload and perform extract/transform/load (ETL)
"""

from pathlib import Path
from uksic.etl.download import Downloader
from uksic.etl.extract import Extractor

# pylint: disable=line-too-long
SIC_SOURCE_REMOTE_URL = 'https://www.ons.gov.uk/file?uri=/methodology/classificationsandstandards/ukstandardindustrialclassificationofeconomicactivities/uksic2007/publisheduksicsummaryofstructureworksheet.xlsx'
DATA_DIR = Path(__file__).parent.parent.parent.parent.joinpath('data')

def run(data_dir: Path | None = None, sic_local_filename: str = 'sic.xlsx'):
    """
    Run application to retrieve remote payload and perform ETL
    """

    if data_dir is None:
        data_dir = DATA_DIR

    sic_source_local_path = data_dir.joinpath(sic_local_filename)

    # Download file if doesn't exist
    downloader = Downloader(src=SIC_SOURCE_REMOTE_URL, dst=sic_source_local_path)
    downloader.download()

    # Extract files if they don't exist
    extractor = Extractor(src_path=sic_source_local_path, dst_dir=DATA_DIR)
    extractor.extract()


if __name__ == "__main__":
    run()
