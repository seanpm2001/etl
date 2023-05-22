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
    log.info("eu_flights_2019_2021.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap: Snapshot = paths.load_dependency("eu_flights_2019_2021.xlsx")

    # Load data from snapshot.
    df = pd.read_excel(snap.path, sheet_name='Data')

    excl_ops_total_2019 = df[['Entity', 'Day 2019',	'Flights 2019 (Reference)']]
    excl_ops_2021 = df[["Entity", "Day", "Flights"]]
    excl_ops_2020 = df[["Entity", "Day Previous Year","Flights Previous Year"]]

    #  2019
    excl_ops_2019_copy = excl_ops_total_2019.copy()
    excl_ops_2019_copy.loc[:, 'Date'] = pd.to_datetime(excl_ops_2019_copy["Day 2019"])
    excl_ops_2019_copy.drop(columns = "Day 2019", inplace = True)
    # Extract month and year from the date column
    excl_ops_2019_copy['Month'] = excl_ops_2019_copy['Date'].dt.month
    excl_ops_2019_copy['Year'] = excl_ops_2019_copy['Date'].dt.year
    # Drop the original date column
    excl_ops_2019_copy = excl_ops_2019_copy.drop('Date', axis=1)
    excl_ops_2019_copy.rename(columns = {'Flights 2019 (Reference)': 'Flights'}, inplace = True)
    excl_ops_2019_copy = excl_ops_2019_copy[excl_ops_2019_copy['Year'] == 2019]


    #  2020
    excl_ops_2020_copy = excl_ops_2020.copy()
    excl_ops_2020_copy.loc[:, 'Date'] = pd.to_datetime(excl_ops_2020_copy["Day Previous Year"])
    excl_ops_2020_copy.drop(columns = "Day Previous Year", inplace = True)
    # Extract month and year from the date column
    excl_ops_2020_copy['Month'] = excl_ops_2020_copy['Date'].dt.month
    excl_ops_2020_copy['Year'] = excl_ops_2020_copy['Date'].dt.year
    # Drop the original date column
    excl_ops_2020_copy = excl_ops_2020_copy.drop('Date', axis=1)
    excl_ops_2020_copy.rename(columns = {'Flights Previous Year': 'Flights'}, inplace = True)
    excl_ops_2020_copy = excl_ops_2020_copy[excl_ops_2020_copy['Year'] == 2020]


    #  2021
    excl_ops_2021_copy = excl_ops_2021.copy()
    excl_ops_2021_copy.loc[:, 'Date'] = pd.to_datetime(excl_ops_2021_copy["Day"])
    excl_ops_2021_copy.drop(columns = "Day", inplace = True)
    # Extract month and year from the date column
    excl_ops_2021_copy['Month'] = excl_ops_2021_copy['Date'].dt.month
    excl_ops_2021_copy['Year'] = excl_ops_2021_copy['Date'].dt.year
    # Drop the original date column
    excl_ops_2021_copy = excl_ops_2021_copy.drop('Date', axis=1)
    excl_ops_2021_copy = excl_ops_2021_copy[excl_ops_2021_copy['Year'] == 2021]

    concatenated_df = pd.concat([excl_ops_2019_copy, excl_ops_2020_copy, excl_ops_2021_copy])
    concatenated_df.rename(columns = {'Entity': 'country'}, inplace = True)
    concatenated_df.reset_index(inplace = True)
    concatenated_df.drop(columns = 'index', inplace = True)

    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = Table(concatenated_df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("eu_flights_2019_2021.end")
