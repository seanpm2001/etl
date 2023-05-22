"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("eu_flights_2022.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap: Snapshot = paths.load_dependency("eu_flights_2022.xlsx")

    # Load data from snapshot.
    df = pd.read_excel(snap.path,sheet_name='Data')
    excl_ops_2022 = df[["Entity", "Day", "Flights"]]

    #  2022
    excl_ops_2022_copy = excl_ops_2022.copy()
    excl_ops_2022_copy.loc[:, 'Date'] = pd.to_datetime(excl_ops_2022_copy["Day"])
    excl_ops_2022_copy.drop(columns = "Day", inplace = True)
    # Extract month and year from the date column
    excl_ops_2022_copy['Month'] = excl_ops_2022_copy['Date'].dt.month
    excl_ops_2022_copy['Year'] = excl_ops_2022_copy['Date'].dt.year
    # Drop the original date column
    excl_ops_2022_copy = excl_ops_2022_copy.drop('Date', axis=1)
    excl_ops_2022_copy = excl_ops_2022_copy[excl_ops_2022_copy['Year'] == 2022]
    excl_ops_2022_copy.rename(columns = {'Entity': 'country'}, inplace = True)
    excl_ops_2022_copy.reset_index(inplace = True)
    excl_ops_2022_copy.drop(columns = 'index', inplace = True)

    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = Table(excl_ops_2022_copy, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("eu_flights_2022.end")
