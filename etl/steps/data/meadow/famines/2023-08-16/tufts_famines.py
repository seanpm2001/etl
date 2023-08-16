"""Load a snapshot and create a meadow dataset."""

from typing import cast

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = cast(Snapshot, paths.load_dependency("tufts_famines.csv"))
    # Load data from snapshot.
    df = pd.read_csv(snap.path)

    #
    # Process data.
    #

    # Step 1: Split the 'Years' column into 'Start Year' and 'End Year'
    df[["Start Year", "End Year"]] = df["Years"].str.split("-", expand=True)
    df["Start Year"] = df["Start Year"].str.strip()
    df["End Year"] = df["End Year"].str.strip()

    # Step 2: Extract 'Estimate', 'Lower Bound', and 'Upper Bound' from the 'Deaths' column
    df["Deaths"] = df["Deaths"].str.replace(",", "")

    # Initial extraction
    df[["Estimate", "Upper Bound"]] = df["Deaths"].str.split(r"–|-", expand=True)

    # If 'Upper Bound' is NaN, set 'Lower Bound' and 'Upper Bound' as the same as 'Estimate'
    df["Lower Bound"] = df["Estimate"]
    df["Upper Bound"] = df["Upper Bound"].fillna(df["Estimate"])

    # Handle the conversion
    df["Estimate"] = df["Estimate"].apply(convert_to_million)
    df["Lower Bound"] = df["Lower Bound"].apply(convert_to_million)
    df["Upper Bound"] = df["Upper Bound"].apply(convert_to_million)
    df["End Year"] = df.apply(transform_end_year, axis=1)

    df = fill_events_manually(df)

    df = df[
        ["Country", "Cause", "Deaths", "Source", "Start Year", "End Year", "Estimate", "Upper Bound", "Lower Bound"]
    ]

    df["Start Year"] = df["Start Year"].astype(int)
    df["End Year"] = df["End Year"].astype(float)

    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)

    tb = tb.set_index(["country", "start_year", "end_year"], verify_integrity=True)

    #
    # Save outputs.
    #

    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()


def convert_to_million(value):
    """
    Convert a string to its equivalent in millions.
    """
    # If the value is already a float, return it
    if isinstance(value, float):
        return value

    # Strip spaces and extract the primary value before any space or parentheses
    value = value.strip().split()[0]

    # Check if the value ends with "m" --> million
    if value[-1] == "m":
        return float(value[:-1]) * 1e6
    if "." in value:
        return float(value) * 1e6

    # If no specific format is given, just convert it to float
    return float(value)


# Function to transform End Year based on Start Year
def transform_end_year(row):
    start_year = row["Start Year"]
    end_year = row["End Year"]

    # Check if end_year is not None and not NaN
    if end_year is not None and not pd.isna(end_year) and len(str(end_year)) == 2:
        return start_year[:2] + str(end_year)
    elif end_year is not None and not pd.isna(end_year) and len(str(end_year)) == 1:
        return start_year[:3] + str(end_year)
    else:
        return end_year


def fill_events_manually(df):
    df.loc[(df["Years"] == "1870s") & (df["Country"] == "India"), "Start Year"] = "1876"
    df.loc[(df["Years"] == "1870s") & (df["Country"] == "India"), "End Year"] = "1878"
    # Causes to filter by
    causes_to_filter = [
        "Starvation of Russian POW’s by the Wehrmacht",
        "Siege of Leningrad",
        "Deaths of Soviet Citizens due to starvation in the USSR, including those killed in the occupation of Kiev and Kharkiv",
        "Death of residents of the Warsaw Ghetto from starvation",
    ]

    # Filter the DataFrame based on the given causes
    filtered_rows = df[df["Cause"].isin(causes_to_filter)]

    # Calculate the sums
    sum_estimate = filtered_rows["Upper Bound"].sum()
    sum_lower_bound = filtered_rows["Lower Bound"].sum()
    sum_upper_bound = filtered_rows["Upper Bound"].sum()

    # Create a new row with the provided attributes and placeholder sums
    new_row = {
        "Country": "Germany/USSR",
        "Cause": "Hunger Plan",
        "Start Year": 1941,
        "End Year": 1945,
        "Estimate": sum_estimate,  # Placeholder
        "Lower Bound": sum_lower_bound,  # Placeholder
        "Upper Bound": sum_upper_bound,  # Placeholder
    }

    # Append the new row to the original DataFrame
    df = df.append(new_row, ignore_index=True)
    df = df[~df["Cause"].isin(causes_to_filter)]

    df.loc[
        (df["Start Year"] == "1934, 1936"),
        "Start Year",
    ] = "1934"

    df.loc[
        (df["End Year"] == "1934, 1936"),
        "Start Year",
    ] = "1936"

    return df
