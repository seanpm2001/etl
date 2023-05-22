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
    log.info("co2_air_transport.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap: Snapshot = paths.load_dependency("co2_air_transport.csv")

    # Load data from snapshot.
    df = pd.read_csv(snap.path, low_memory=False)
    cos_to_use = ['Country', 'FLIGHT' ,'Frequency', 'SOURCE', 'TIME', 'Value']
    cos_to_name = ['country', 'flight_type' ,'frequency', 'emission_source', 'year', 'value']
    df = df[cos_to_use]
    df = df.rename(columns=dict(zip(df.columns, cos_to_name)))

    df['year'] = pd.to_datetime(df['year'])  # Convert the TIME column to datetime
    df['Month'] = df['year'].dt.month  # Extract month and create a new column
    df['Year'] = df['year'].dt.year  # Extract month and create a new column
    df.drop(['year'], axis=1, inplace= True)

        #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("co2_air_transport.end")
