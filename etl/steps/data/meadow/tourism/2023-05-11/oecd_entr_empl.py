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
    log.info("oecd_entr_empl.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap: Snapshot = paths.load_dependency("oecd_entr_empl.csv")

    # Load data from snapshot.
    df = pd.read_csv(snap.path)


    columns_to_keep = ['Country','Key indicator', 'Year', 'Value']
    df = df[columns_to_keep]
    df = pd.pivot_table(df, values='Value', index=['Country', 'Year'], columns=['Key indicator'])
    assert df.index.is_unique, "Index is not well constructed"
    df.reset_index(inplace = True)
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

    log.info("oecd_entr_empl.end")
