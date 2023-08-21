"""Load a meadow dataset and create a garden dataset."""

from typing import cast

from owid.catalog import Dataset, Table, Variable, VariableMeta
from owid.catalog.utils import underscore

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset for mortality database and UN WPP for population.
    ds_garden = cast(Dataset, paths.load_dependency("mortality_database"))
    ds_wpp = cast(Dataset, paths.load_dependency("un_wpp"))

    #
    # Process data.
    # Read mortality table from UN WPP dataset.
    tb_mort = ds_wpp["mortality"]
    tb_mort = tb_mort.reset_index()

    tb_mort = tb_mort[
        (tb_mort["sex"] == "all")
        & (tb_mort["age"] == "all")
        & (tb_mort["variant"] == "estimates")
        & (tb_mort["metric"] == "deaths")
    ]
    tb_mort = tb_mort.drop(columns=["sex", "age", "variant", "metric"])
    tb_mort = tb_mort.rename(columns={"location": "country", "value": "total_estimated_deaths"})
    #
    #
    tb = ds_garden["all_causes__both_sexes__all_ages"].reset_index()
    tb = tb.drop(
        columns=[
            "share_of_total_deaths_in_both_sexes_aged_all_ages_years_that_are_from_all_causes",
            "age_standardized_deaths_that_are_from_all_causes_per_100_000_people__in_both_sexes_aged_all_ages",
            "deaths_from_all_causes_per_100_000_people_in__both_sexes_aged_all_ages",
        ]
    )

    tb_merge = tb.merge(tb_mort, on=["country", "year"], how="left")
    assert all(
        tb_merge["total_estimated_deaths"]
        <= tb_merge["total_deaths_that_are_from_all_causes__in_both_sexes_aged_all_ages"]
    )

    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)

    # Save changes in the new garden dataset.
    ds_garden.save()


def calculate_share_of_cause_specific_deaths_out_of_total_deaths(tb: Table, tb_mort: Table) -> Table:
    """
    Using UN WPP estimated deaths as the denominator we calculate the share of cause specific deaths out of total deaths.
    Cause-specific deaths includes ill-defined diseases.
    We only need to calculate this for all age-groups
    """
    mask = (tb["age_group"] == "all ages") & (tb["cause"] == "All Causes") & (tb["sex"] == "Both sexes")
    tb_mask = tb[mask]
    tb_mask.merge(tb_mort, on=["country", "year"], how="left")
