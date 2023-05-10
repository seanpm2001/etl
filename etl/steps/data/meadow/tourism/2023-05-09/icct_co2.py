"""Load a snapshot and create a meadow dataset."""
import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot
import numpy as np
import difflib

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


def process_sheet(excel_object: pd.ExcelFile, sheet_name: str, latest_year: int) -> pd.DataFrame:
    """
    Process a sheet from the given Excel file and return a cleaned DataFrame.

    Args:
        excel_object (pd.ExcelFile): The loaded Excel file to process.
        sheet_name (str): The name of the sheet to process from the Excel file.
        year_range (tuple): A tuple with 2 elements (start_year, end_year) indicating the years to include in the output DataFrame.

    Returns:
        pd.DataFrame: The cleaned and processed DataFrame.
    """

    # Read the sheet from the Excel file
    df = pd.read_excel(excel_object, sheet_name=sheet_name)

        # Drop unnecessary columns
    latest_year = str(latest_year)
    # Find the row where the first column is "- Total"
    start_idx = df.index[df.iloc[:, 0] == "Total"][0]

    # Slice the DataFrame from that row onwards
    df = df.loc[:start_idx, :]
    df.dropna(how='all', axis=1, inplace=True)
    df.dropna(how='all', axis=0, inplace=True)
    df.columns
    df = df.set_index('Unnamed: 0')
    prev_col_name = ''
    for col in df.columns:
        if col.startswith('Unnamed'):
            df = df.rename(columns={col: prev_col_name})
        else:
            prev_col_name = col
    df_t = df.T
    df_t = df_t.rename(columns={'Departure Country': 'year'})
    df_t.index = df_t.index.set_names(['indicator'])
    df_t = df_t.reset_index()
    df_melted = pd.melt(df_t, id_vars=['year', 'indicator'], var_name='country', value_name='value')
    df_melted['year'] = df_melted['year'].fillna(latest_year)
    df_melted['year'] = df_melted['year'].astype(str)
    df_melted.loc[df_melted['year'].str.contains('-'), 'indicator'] = df_melted.loc[df_melted['year'].str.contains('-'), 'indicator'] + '%'
    df_melted['year'] = df_melted['year'].str.replace('.*-', '', regex=True)
    df_melted['year'] = df_melted['year'].astype(str).str.replace('.0', '', regex=True)
    df_melted['year'] = (df_melted['year'].astype(int) + 2000).astype(str)
    df_melted['indicator'] = sheet_name + '-' + df_melted['indicator'].astype(str)

    df_melted = df_melted.set_index(['year', 'indicator', 'country'])

    assert df.index.is_unique, f"Index is not unique in sheet '{sheet_name}'." # Added assert statement to check index is unique

    return df_melted

def process_data(excel_object: pd.ExcelFile, latest_year: int, matched_sheet_names: list) -> pd.DataFrame:
    """
    Process sheets of interest in the given Excel file and return a combined DataFrame.

    Args:
        excel_object (pd.ExcelFile): The loaded Excel file to process.
        year_range (tuple): A tuple with 2 elements (start_year, end_year) indicating the years to include in the output DataFrame.

    Returns:
        pd.DataFrame: The combined and processed DataFrame from all sheets.
    """

    data_frames = []
    # Iterate through the matched sheet names and process each sheet
    for i, sheet_name in enumerate(matched_sheet_names):
        print(f"Processing sheet: {sheet_name}")
        df = process_sheet(excel_object, sheet_name, latest_year)
        data_frames.append(df)

    # Concatenate all the processed DataFrames
    df_concat = pd.concat(data_frames, axis=0)
    df_concat.reset_index(inplace=True)

    # Pivot the DataFrame to have 'indicator' as columns and 'value' as cell values
    df_concat = df_concat.set_index(['year', 'indicator', 'country'])

    assert df_concat.index.is_unique, "The index in the concatenated DataFrame is not unique."


    return df_concat


def run(dest_dir: str) -> None:
    log.info("icct_co2.start")

    # Ask the user for the year range
    latest_year = int(input("Enter the most recent year: "))

    # Load inputs.
    snap: Snapshot = paths.load_dependency("icct_co2.xlsx")
    excel_object = load_data(snap)

    if excel_object is None:
        return

    # Get the list of sheet names in the Excel file
    sheet_names = excel_object.sheet_names
    print(sheet_names)
    log.info(f"Found {len(sheet_names)} sheets in the Excel file:")

    sheet_names_to_load = ["Total Operations",  "Domestic Operations", "International Operations"]


    log.info(f"Loading {len(sheet_names_to_load)} sheets from the Excel file:")

    # Match the sheet names to load to the available sheet names
    matched_sheet_names = []
    for target_sheet_name in sheet_names_to_load:
        best_match = None
        best_ratio = 0
        for sheet_name in sheet_names:
            ratio = difflib.SequenceMatcher(None, target_sheet_name, sheet_name).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_match = sheet_name
        if best_ratio >= 0.6:  # You can adjust this threshold based on your requirements
            matched_sheet_names.append(best_match)

    # Check that all required sheets were matched

    print("Matched sheet names:")
    for name in matched_sheet_names:
        print(f"- {name}")
        confirm = input("Are the matched sheet names correct? (y/n)").lower()
        if confirm != "y":
            # If the matched sheet names are not correct, raise an error
            raise ValueError("Matched sheet names are incorrect.")

    # Process data.
    df_concat = process_data(excel_object, latest_year, matched_sheet_names)
    df_concat = df_concat.reset_index()
    df_concat['value'] = df_concat['value'].replace('---', np.nan)

    df_concat['year'] = df_concat['year'].astype(int)
    df_concat['value'] = df_concat['value'].astype(float)
    df_concat['country'] = df_concat['country'].astype(str)
    df_concat['indicator'] = df_concat['indicator'].astype(str)
    df_concat = pd.pivot_table(df_concat, values='value', index=['year', 'country'], columns=['indicator'])

    # Create a new table and ensure all columns are snake-case.
    tb = Table(df_concat, short_name=paths.short_name, underscore=True)

    # Save outputs.
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("icct_co2.end")
