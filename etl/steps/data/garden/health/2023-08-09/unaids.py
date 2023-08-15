"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import numpy as np
import owid.catalog.processing as pr
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Logger
log = get_logger()


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("unaids"))
    # Auxiliary dataset, contains HIV prevalence data for children (0-14)
    ds_meadow_aux = cast(Dataset, paths.load_dependency("unaids_hiv_children"))

    # Load population dataset.
    ds_population: Dataset = paths.load_dependency("population")

    # Read tables from meadow datasets.
    tb = ds_meadow["unaids"].reset_index()
    tb_aux = ds_meadow_aux["unaids_hiv_children"].reset_index()

    #
    # Process data.
    #
    log.info("health.unaids: handle NaNs")
    tb = handle_nans(tb)

    # Harmonize country names (main table)
    log.info("health.unaids: harmonize countries (main table)")
    tb: Table = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Pivot table
    log.info("health.unaids: pivot table")
    tb = tb.pivot(
        index=["country", "year", "subgroup_description"], columns="indicator", values="obs_value"
    ).reset_index()

    # Underscore column names
    log.info("health.unaids: underscore column names")
    tb = tb.underscore()

    # Harmonize country names (auxiliary table)
    log.info("health.unaids: harmonize countries (aux table)")
    tb_aux: Table = geo.harmonize_countries(
        df=tb_aux, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Combine tables
    log.info("health.unaids: combine tables")
    tb = pr.concat([tb, tb_aux], ignore_index=True)

    # Rename columns
    log.info("health.unaids: rename columns")
    tb = tb.rename(columns={"subgroup_description": "disaggregation"})

    # Dtypes
    log.info("health.unaids: set dtypes")
    tb = tb.astype(
        {
            "domestic_spending_fund_source": float,
            "hiv_prevalence": float,
            "deaths_averted_art": float,
            "aids_deaths": float,
        }
    )

    # Add per_capita
    log.info("health.unaids: add per_capita")
    tb = add_per_capita_variables(tb, ds_population)

    # Set index
    log.info("health.unaids: set index")
    tb = tb.set_index(["country", "year", "disaggregation"], verify_integrity=True)

    # Drop all NaN rows
    tb = tb.dropna(how="all")

    # Set table's short_name
    tb.metadata.short_name = paths.short_name

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def handle_nans(tb: Table) -> Table:
    """Handle NaNs in the dataset.

    - Replace '...' with NaN
    - Ensure no NaNs for non-textual data
    - Drop NaNs & check that all textual data has been removed
    """
    # Replace '...' with NaN
    tb["obs_value"] = tb["obs_value"].replace("...", np.nan)
    # Ensure no NaNs for non-textual data
    assert not tb.loc[-tb["is_textualdata"], "obs_value"].isna().any(), "NaN values detected for not textual data"
    # Drop NaNs & check that all textual data has been removed
    tb = tb.dropna(subset="obs_value")
    assert tb.is_textualdata.sum() == 0, "NaN"

    return tb


def add_per_capita_variables(tb: Table, ds_population: Dataset) -> Table:
    """Add per-capita variables.

    Parameters
    ----------
    tb : Table
        Primary data.
    ds_population : Dataset
        Population dataset.

    Returns
    -------
    tb : Table
        Data after adding per-capita variables.

    """
    tb = tb.copy()

    # Estimate per-capita variables.
    ## Only consider variable "domestic_spending_fund_source"
    mask = tb["domestic_spending_fund_source"].isna()

    ## Add population variable
    # print("----------------")
    # tb_fund = geo.add_population_to_table(tb[~mask], ds_population, expected_countries_without_population=[])
    tb_fund = geo.add_population_to_dataframe(tb[~mask], ds_population, expected_countries_without_population=[])
    ## Estimate ratio
    tb_fund["domestic_spending_fund_source_per_capita"] = (
        tb_fund["domestic_spending_fund_source"] / tb_fund["population"]
    )

    ## Combine tables again
    tb = pr.concat([tb_fund, tb[mask]], ignore_index=True)

    # Drop unnecessary column.
    tb = tb.drop(columns=["population"])

    return tb