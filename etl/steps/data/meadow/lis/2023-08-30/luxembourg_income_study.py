"""
Load the three LIS snapshots and creates three tables in the luxembourg_income_study meadow dataset.
Country names are converted from iso-2 codes in this step and years are reformated from "YY" to "YYYY"
"""

from typing import cast

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder
from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Create a dictionary with the names of the snapshots and their id variables
snapshots_dict = {
    "lis_keyvars": ["country", "year", "dataset", "variable", "eq"],
    "lis_abs_poverty": ["country", "year", "dataset", "variable", "eq", "povline"],
    "lis_distribution": ["country", "year", "dataset", "variable", "eq", "percentile"],
    "lis_percentiles": ["country", "year", "dataset", "variable", "eq", "percentile"],
}

# Define a dictionary with the age suffixes for the different snapshots
age_dict = {"all": "", "adults": "_adults"}

# Define a list of datasets to drop
# NOTE: These datasets are dropped because LIS decided to not show key indicators. From a conversation with LIS:
"""
AT87 is still accessible for ongoing research, but we advise not to use it.
The years FR78/FR89/FR94 are not wrong per-se, but inequality numbers are rather different from the HBS based series, and we are not promoting both series at this stage. So all data points available through lissydata are based on the taxregister based series.
DE81 is not in line with the EVS data, nor GSOEP (so we equally do not promote these numbers.
ML13, ML11 or SE67 have high proportion of 0s and/or missing values in DHI, so far we do not show statistics for those.
"""

drop_datasets_list = ["AT87", "DE81", "FR78", "FR89", "FR94", "ML11", "ML13", "SE67"]


def run(dest_dir: str) -> None:
    log.info("luxembourg_income_study.start")

    #
    # Load inputs.
    # Load reference file with country names in OWID standard
    df_countries_regions = cast(Dataset, paths.load_dependency("regions"))["regions"][["name", "iso_alpha2"]]

    # Create a new meadow dataset with the same metadata as the snapshot.
    snap = paths.load_dependency("lis_keyvars.csv")
    ds_meadow = Dataset.create_empty(dest_dir, metadata=convert_snapshot_metadata(snap.metadata))

    # Ensure the version of the new dataset corresponds to the version of current step.
    ds_meadow.metadata.version = paths.version
    ds_meadow.metadata.short_name = "luxembourg_income_study"

    for age, age_suffix in age_dict.items():
        for ds_name, ds_ids in snapshots_dict.items():
            # Retrieve snapshot.
            snap: Snapshot = paths.load_dependency(f"{ds_name}{age_suffix}.csv")

            # Load data from snapshot.
            df = pd.read_csv(snap.path)

            df[[col for col in df.columns if col not in ds_ids]] = df[
                [col for col in df.columns if col not in ds_ids]
            ].apply(pd.to_numeric, errors="coerce")

            # Drop datasets LIS is not promoting
            df = df[~df["dataset"].isin(drop_datasets_list)].reset_index(drop=True)

            # Extract country and year from dataset
            df["country"] = df["dataset"].str[:2].str.upper()
            df["year"] = df["dataset"].str[2:4].astype(int)

            # Replace "UK" with "GB" (official ISO-2 name for the United Kingdom)
            df["country"] = np.where(df["country"] == "UK", "GB", df["country"])

            # Create year variable in the format YYYY instead of YY
            df["year"] = np.where(df["year"] > 50, df["year"] + 1900, df["year"] + 2000)

            # Merge dataset and country dictionary to get the name of the country (and rename it as "country")
            df = pd.merge(df, df_countries_regions, left_on="country", right_on="iso_alpha2", how="left")
            df = df.drop(columns=["country", "iso_alpha2"])
            df = df.rename(columns={"name": "country"})

            # Move country and year to the beginning
            cols_to_move = ["country", "year"]
            df = df[cols_to_move + [col for col in df.columns if col not in cols_to_move]]

            # Set indices and sort.
            df = df.set_index(ds_ids, verify_integrity=True).sort_index()

            # Create a new table and ensure all columns are snake-case.
            tb = Table(df, short_name=f"{ds_name}{age_suffix}", underscore=True)

            # Add the new table to the meadow dataset.
            ds_meadow.add(tb)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("luxembourg_income_study.end")
