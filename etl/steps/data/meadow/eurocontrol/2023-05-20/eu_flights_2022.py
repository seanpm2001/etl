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

    # Load data from snapshot file into a dataframe
    df = pd.read_excel(snap.path,sheet_name='Data')

    # Select required columns for 2022 data
    excl_ops_2022 = df[["Entity", "Day", "Flights"]]

    # Create a copy of the 2022 data
    excl_ops_2022_copy = excl_ops_2022.copy()

    # Convert 'Day' column to datetime format
    excl_ops_2022_copy.loc[:, 'Date'] = pd.to_datetime(excl_ops_2022_copy["Day"])

    # Drop the original 'Day' column
    excl_ops_2022_copy.drop(columns = "Day", inplace = True)

    # Extract month and year from the 'Date' column
    excl_ops_2022_copy['Month'] = excl_ops_2022_copy['Date'].dt.month
    excl_ops_2022_copy['Year'] = excl_ops_2022_copy['Date'].dt.year

    # Drop the 'Date' column after extracting month and year
    excl_ops_2022_copy = excl_ops_2022_copy.drop('Date', axis=1)

    # Filter data to retain only 2022 data
    excl_ops_2022_copy = excl_ops_2022_copy[excl_ops_2022_copy['Year'] == 2022]

    # Rename 'Entity' column to 'country'
    excl_ops_2022_copy.rename(columns = {'Entity': 'country'}, inplace = True)

    # Reset index of the dataframe
    excl_ops_2022_copy.reset_index(inplace = True)

    # Drop the old index column
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
