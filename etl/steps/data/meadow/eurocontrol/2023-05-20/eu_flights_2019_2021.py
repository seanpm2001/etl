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

    #
    # Process data.
    #
    excl_ops_total_2019 = df[['Entity', 'Day 2019', 'Flights 2019 (Reference)']]
    excl_ops_2021 = df[["Entity", "Day", "Flights"]]
    excl_ops_2020 = df[["Entity", "Day Previous Year","Flights Previous Year"]]

    # Process data for the years 2019, 2020, 2021
    excl_ops_2019_copy = process_year_data(excl_ops_total_2019, "Day 2019", 'Flights 2019 (Reference)', 2019)
    excl_ops_2020_copy = process_year_data(excl_ops_2020, "Day Previous Year", 'Flights Previous Year', 2020)
    excl_ops_2021_copy = process_year_data(excl_ops_2021, "Day", 'Flights', 2021)

    # Concatenate the processed data
    concatenated_df = pd.concat([excl_ops_2019_copy, excl_ops_2020_copy, excl_ops_2021_copy])

    # Rename 'Entity' column to 'country'
    concatenated_df.rename(columns = {'Entity': 'country'}, inplace = True)

    # Reset index and remove the original index column
    concatenated_df.reset_index(inplace = True, drop=True)

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



def process_year_data(df, date_column, flights_column, year):
    """
    This function processes a dataframe by performing several steps: extracting date details,
    renaming columns, and filtering data based on the year.

    Parameters:
    df (pd.DataFrame): Input dataframe to be processed
    date_column (str): The name of the column in the dataframe that contains dates
    flights_column (str): The name of the column in the dataframe that contains flight data
    year (int): The year to filter the dataframe on

    Returns:
    df_copy (pd.DataFrame): The processed dataframe, filtered for the specified year
    """

    # Create a copy of the input dataframe to avoid modifying the original
    df_copy = df.copy()

    # Convert the date column to datetime format
    df_copy.loc[:, 'Date'] = pd.to_datetime(df_copy[date_column])

    # Drop the original date column
    df_copy.drop(columns = date_column, inplace = True)

    # Extract month and year from the Date column
    df_copy['Month'] = df_copy['Date'].dt.month
    df_copy['Year'] = df_copy['Date'].dt.year

    # Drop the Date column
    df_copy = df_copy.drop('Date', axis=1)

    # Rename the flights column to 'Flights'
    df_copy.rename(columns = {flights_column: 'Flights'}, inplace = True)

    # Filter the data for the specified year
    return df_copy[df_copy['Year'] == year]
