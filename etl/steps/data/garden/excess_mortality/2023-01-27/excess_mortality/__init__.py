"""Load a meadow dataset and create a garden dataset."""
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder

from .input import build_df
from .process import process_df

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Year range to be used (rest is filtered out)
YEAR_MIN = 2020
YEAR_MAX = 2022  # (kobak_ages does not have data for 2023)


def run(dest_dir: str) -> None:
    log.info("excess_mortality: start")

    #
    # Load inputs.
    #
    # Load dependency datasets.
    ds_hmd: Dataset = paths.load_dependency("hmd_stmf")
    ds_wmd: Dataset = paths.load_dependency("wmd")
    ds_kobak: Dataset = paths.load_dependency("xm_karlinsky_kobak")

    # Build initial dataframe
    df = build_df(ds_hmd, ds_wmd, ds_kobak)

    #
    # Process data.
    #
    log.info("excess_mortality: processing data")
    df = process_df(df)

    # Create a new table with the processed data.
    print(paths.short_name)
    tb_garden = Table(df, short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = Dataset.create_empty(dest_dir)

    # Add table of processed data to the new dataset.
    ds_garden.add(tb_garden)

    # Update dataset and table metadata using the adjacent yaml file.
    print(paths.metadata_path)
    ds_garden.update_metadata(paths.metadata_path)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("excess_mortality: end")
