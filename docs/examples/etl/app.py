from uksic.etl.app import App
from pathlib import Path

# Set up ETL
app = App(
  data_dir=Path(__file__).parent.joinpath('output')
)

# Run, fetching from remote URL
app.run()
