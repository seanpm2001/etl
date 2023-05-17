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


def load_data(snap: Snapshot) -> pd.ExcelFile:
    """
    Load the Excel file from the given snapshot.

    Args:
        snap (Snapshot): The snapshot object containing the path to the Excel file.

    Returns:
        pd.ExcelFile: The loaded Excel file as a pandas ExcelFile object, or None if loading failed.
    """

    # Attempt to load the Excel file from the snapshot path.
    try:
        excel_object = pd.ExcelFile(snap.path)
    except Exception as e:
        # Log an error and return None if loading failed.
        log.error(f"Failed to load Excel file: {e}")
        return None

    # Return the loaded Excel file as a pandas ExcelFile object.
    return excel_object


def run(dest_dir: str) -> None:
    log.info("unwto_environment.start")
    sheet_name_to_load = "Data"
    # Load inputs.
    snap: Snapshot = paths.load_dependency("unwto_environment.xlsx")
    excel_object = load_data(snap)

    if excel_object is None:
        return

    # Check if sheet 'Data' is present in the Excel file
    if "Data" not in excel_object.sheet_names:
        log.warning("Sheet 'Data' not found in the Excel file.")
        return

    # Read the sheet from the Excel file
    df = pd.read_excel(excel_object, sheet_name=sheet_name_to_load)

    df = df[["SeriesDescription", "GeoAreaName", "TimePeriod", "Value", "Time_Detail", "Source"]]
    if df["Time_Detail"].equals(df["TimePeriod"]):
        df.drop("TimePeriod", axis=1, inplace=True)
    df.columns = ["implementation_type", "country", "value", "year", "source"]

    df.set_index(["country", "year", "implementation_type"], inplace=True)
    assert df.index.is_unique, f"Index is not unique'."
    df.reset_index(inplace=True)
    df = pd.pivot_table(df, values='value', index=['country', 'year'], columns=['implementation_type'])
    df.reset_index(inplace=True)
    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name = paths.short_name, underscore = True)

    # Save outputs.
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("unwto_environment.end")
