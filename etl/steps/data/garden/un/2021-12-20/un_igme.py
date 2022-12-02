import json
from typing import List, cast

import pandas as pd
from owid import catalog
from owid.catalog import Dataset, Source, Table
from owid.catalog.utils import underscore_table
from owid.datautils import geo
from structlog import get_logger

from etl.helpers import Names
from etl.paths import DATA_DIR

log = get_logger()

GAPMINDER_CHILD_MORTALITY_DATASET_PATH = DATA_DIR / "open_numbers/open_numbers/latest/gapminder__child_mortality"
GAPMINDER_INFANT_MORTALITY_DATASET_PATH = DATA_DIR / "open_numbers/open_numbers/latest/gapminder__hist_imr"


# naming conventions
N = Names(__file__)


def run(dest_dir: str) -> None:
    log.info("un_igme.start")
    # read dataset from meadow
    ds_meadow = Dataset(DATA_DIR / "meadow/un/2021-12-20/un_igme")
    tb_meadow = ds_meadow["un_igme"]
    df = pd.DataFrame(tb_meadow).drop(columns=["index"])
    # adding source tag to all UN IGME rows prior to combination with Gapminder data
    df["source"] = "UN IGME"
    # Duplicating the rows we will joing with gapminder so we also have the original values from just UN IGME
    df = duplicate_child_infant_mortality(df)
    df_gap = get_gapminder_data(max_year=max(df["year"]))
    df_combine = pd.concat([df, df_gap])

    log.info("un_igme.exclude_countries")
    df_combine = exclude_countries(df_combine)

    log.info("un_igme.harmonize_countries")
    dfc = harmonize_countries(df_combine)
    # Preferentially use UN IGME data where there is duplicate values for country-year combinations
    dfc = combine_datasets(dfc)
    # Calculate missing age-group mortality rates
    dfc = calculate_mortality_rate(dfc)

    # Calculate the absolute number of youth deaths
    dfc = calculate_youth_deaths(dfc)

    # Making the values in the table a bit more appropriate for our use and pivoting to a wide table.
    log.info("un_igme.clean_data")
    dfc = clean_and_format_data(dfc)

    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata

    tb_garden = Table(dfc, metadata=tb_meadow.metadata)
    tb_garden.columns.names = ["metric", "sex", "indicator", "unit"]

    # move sex and metric to dimensions
    tb_garden = tb_garden.stack(level="metric").stack(level="sex")

    # add units to variable metadata
    units = tb_garden.columns.get_level_values("unit")
    tb_garden.columns = tb_garden.columns.droplevel(level="unit")

    for col, unit in zip(tb_garden.columns, units):
        log.info(col)
        tb_garden[col].metadata.unit = unit
        if tb_garden[col].metadata.unit in ["deaths", "stillbirths"]:
            tb_garden[col] = tb_garden[col].astype("Int64").round(0)
        else:
            tb_garden[col] = tb_garden[col].astype("float").round(2)
        # Altering the Source for the few variables we have combined UN IGME and Gapminder
        if tb_garden[col].metadata.title == "Under-five mortality rate - Both sexes - value":
            tb_garden[col].metadata.sources = [
                Source(name="Gapminder (2020); United Nations Inter-agency Group for Child Mortality Estimation (2021)")
            ]
            tb_garden[
                col
            ].metadata.description = "Share of children in the world dying in their first five years of life. This time-series is a combination of Gapminder and UN IGME. We use Gapminder data from 1800, until there is UN IGME data available, the availability of data from UN IGME varies between countries."
        if tb_garden[col].metadata.title == "Infant mortality rate - Both sexes - value":
            tb_garden[col].metadata.sources = [
                Source(name="Gapminder (2015); United Nations Inter-agency Group for Child Mortality Estimation (2021)")
            ]
            tb_garden[
                col
            ].metadata.description = "Share of children in the world dying in their first year of life. This time-series is a combination of Gapminder and UN IGME. We use Gapminder data from 1800, until there is UN IGME data available, the availability of data from UN IGME varies between countries."
    tb_garden = underscore_table(tb_garden)
    ds_garden.add(tb_garden)
    ds_garden.save()
    log.info("un_igme.end")


def duplicate_child_infant_mortality(df: pd.DataFrame) -> pd.DataFrame:
    """Duplicating the Child and Infant mortality rows so we can have one version which is 'original' and one which is a combination of Gapminder and UN IGME"""
    df_copy = df.loc[
        (df["indicator"].isin(["Under-five mortality rate", "Infant mortality rate"])) & (df["sex"] == "Total")
    ]

    df_copy["indicator"] = df_copy["indicator"].astype(str) + " (OMM: Gapminder, IGME)"
    df_copy = df_copy.drop(columns=["lower_bound", "upper_bound"])
    df = pd.concat([df, df_copy])
    return df


def combine_datasets(df: pd.DataFrame) -> pd.DataFrame:
    """
    Combine the UN IGME and the Gapminder datasets with a preference for the IGME data
     - Split dataframe into duplicate and non-duplicate rows (duplicate on country, indicator, sex, year)
     - Remove the Gapminder rows in the duplicated data
     - Recombine the two datasets
     - Check there are no longer any duplicates
    """

    no_dups_df = pd.DataFrame(df[~df.duplicated(subset=["country", "indicator", "sex", "year"], keep=False)])
    keep_igme = pd.DataFrame(
        df[(df.duplicated(subset=["country", "indicator", "sex", "year"], keep=False)) & (df.source == "UN IGME")]
    )
    assert keep_igme.indicator.isin(
        ["Infant mortality rate (OMM: Gapminder, IGME)", "Under-five mortality rate (OMM: Gapminder, IGME)"]
    ).all()

    df_clean = pd.concat([no_dups_df, keep_igme], ignore_index=True)
    # Convert from per 1000 to %
    df_clean.loc[
        df_clean.indicator.isin(
            ["Infant mortality rate (OMM: Gapminder, IGME)", "Under-five mortality rate (OMM: Gapminder, IGME)"]
        ),
        "value",
    ] = (
        df_clean.loc[
            df_clean.indicator.isin(
                ["Infant mortality rate (OMM: Gapminder, IGME)", "Under-five mortality rate (OMM: Gapminder, IGME)"]
            ),
            "value",
        ]
        / 10
    )
    df_clean.loc[
        df_clean.indicator.isin(
            ["Infant mortality rate (OMM: Gapminder, IGME)", "Under-five mortality rate (OMM: Gapminder, IGME)"]
        ),
        "lower_bound",
    ] = (
        df_clean.loc[
            df_clean.indicator.isin(
                ["Infant mortality rate (OMM: Gapminder, IGME)", "Under-five mortality rate (OMM: Gapminder, IGME)"]
            ),
            "lower_bound",
        ]
        / 10
    )
    df_clean.loc[
        df_clean.indicator.isin(
            ["Infant mortality rate (OMM: Gapminder, IGME)", "Under-five mortality rate (OMM: Gapminder, IGME)"]
        ),
        "upper_bound",
    ] = (
        df_clean.loc[
            df_clean.indicator.isin(
                ["Infant mortality rate (OMM: Gapminder, IGME)", "Under-five mortality rate (OMM: Gapminder, IGME)"]
            ),
            "upper_bound",
        ]
        / 10
    )
    df_clean.loc[
        df_clean.indicator.isin(
            ["Infant mortality rate (OMM: Gapminder, IGME)", "Under-five mortality rate (OMM: Gapminder, IGME)"]
        ),
        "unit",
    ] = "deaths per 100 live births"
    assert df_clean[df_clean.groupby(["country", "indicator", "sex", "year"]).transform("size") > 1].shape[0] == 0
    df_clean = calculate_surivivors(df_clean)
    return df_clean


def calculate_surivivors(df: pd.DataFrame) -> pd.DataFrame:
    # Calculating the long-run values of those surviving first 1 and 5 years of life
    df_survive = df[
        df.indicator.isin(
            ["Infant mortality rate (OMM: Gapminder, IGME)", "Under-five mortality rate (OMM: Gapminder, IGME)"]
        )
    ]
    df_survive["value"] = 100 - df_survive["value"]
    df_survive["lower_bound"] = 100 - df_survive["lower_bound"]
    df_survive["upper_bound"] = 100 - df_survive["upper_bound"]
    df_survive.loc[
        df_survive["indicator"] == "Infant mortality rate (OMM: Gapminder, IGME)", ["unit"]
    ] = "share surviving first year of life"
    df_survive.loc[
        df_survive["indicator"] == "Under-five mortality rate (OMM: Gapminder, IGME", ["unit"]
    ] = "share surviving first five years of life"

    df_survive["indicator"] = df_survive["indicator"].replace(
        {
            "Under-five mortality rate (OMM: Gapminder, IGME)": "Share surviving first five years of life",
            "Infant mortality rate (OMM: Gapminder, IGME)": "Share surviving first year of life",
        }
    )
    df_survive["source"] = "OWID based on Gapminder and IGME"
    df_out = pd.concat([df, df_survive])
    return df_out


def get_gapminder_data(max_year: int) -> pd.DataFrame:
    """
    Get child and infant mortality data from open numbers
    """
    gapminder_cm_df = catalog.Dataset(GAPMINDER_CHILD_MORTALITY_DATASET_PATH)
    gapminder_child_mort = pd.DataFrame(
        gapminder_cm_df["child_mortality_0_5_year_olds_dying_per_1000_born"]
    ).reset_index()
    gapminder_child_mort["indicator"] = "Under-five mortality rate (OMM: Gapminder, IGME)"
    gapminder_child_mort["sex"] = "Total"
    gapminder_child_mort["unit"] = "Deaths per 1000 live births"
    gapminder_child_mort = gapminder_child_mort.rename(
        columns={"geo": "country", "time": "year", "child_mortality_0_5_year_olds_dying_per_1000_born": "value"}
    )
    gapminder_child_mort = gapminder_child_mort[gapminder_child_mort["year"] <= max_year]

    # get infant mortality from open numbers
    gapminder_inf_m_df = catalog.Dataset(GAPMINDER_INFANT_MORTALITY_DATASET_PATH)
    gapminder_inf_mort = pd.DataFrame(gapminder_inf_m_df["infant_mortality_rate"]).reset_index()
    gapminder_inf_mort["indicator"] = "Infant mortality rate (OMM: Gapminder, IGME)"
    gapminder_inf_mort["sex"] = "Total"
    gapminder_inf_mort["unit"] = "Deaths per 1000 live births"
    gapminder_inf_mort = gapminder_inf_mort.rename(columns={"area": "country", "infant_mortality_rate": "value"})

    df_gapminder = pd.concat([gapminder_child_mort, gapminder_inf_mort])
    df_gapminder["source"] = "Gapminder"
    # Removing rows with NA values
    df_gapminder = df_gapminder.dropna(subset="value")
    return df_gapminder


def load_excluded_countries() -> List[str]:
    with open(N.excluded_countries_path, "r") as f:
        data = json.load(f)
        assert isinstance(data, list)
    return data


def exclude_countries(df: pd.DataFrame) -> pd.DataFrame:
    excluded_countries = load_excluded_countries()
    return cast(pd.DataFrame, df.loc[~df.country.isin(excluded_countries)])


def harmonize_countries(df: pd.DataFrame) -> pd.DataFrame:
    unharmonized_countries = df["country"]
    df = geo.harmonize_countries(df=df, countries_file=str(N.country_mapping_path))

    missing_countries = set(unharmonized_countries[df.country.isnull()])
    if any(missing_countries):
        raise RuntimeError(
            "The following raw country names have not been harmonized. "
            f"Please: (a) edit {N.country_mapping_path} to include these country "
            f"names; or (b) add them to {N.excluded_countries_path}."
            f"Raw country names: {missing_countries}"
        )

    return df


def clean_and_format_data(df: pd.DataFrame) -> pd.DataFrame:
    """Cleaning up the values and dropping unused columns"""
    df = df.drop(columns=["regional_group"])
    df = df[
        ~df["indicator"].isin(
            ["Progress towards SDG in neonatal mortality rate", "Progress towards SDG in under-five mortality rate"]
        )
    ]
    df["unit"] = df["unit"].replace(
        {
            "Number of deaths": "deaths",
            "Deaths per 1000 live births": "deaths per 1,000 live births",
            "Number of stillbirths": "stillbirths",
        }
    )
    df["sex"] = df["sex"].replace({"Total": "Both sexes"})

    df = df.pivot(
        index=["country", "year"], columns=["sex", "indicator", "unit"], values=["value", "lower_bound", "upper_bound"]
    )
    df = df.dropna(how="all", axis=1)
    return df


def calculate_mortality_rate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculating  mortality rates for missing age-groups. For each key in the dictionary below we calculate the mortality rate using the items for each key
    """
    output_df = []
    rate_calculations = {
        "Under-ten mortality rate": ["Under-five mortality rate", "Mortality rate age 5-9"],
        "Under-fifteen mortality rate": ["Under-five mortality rate", "Mortality rate age 5-14"],
        "Under-twenty-five mortality rate": ["Under-five mortality rate", "Mortality rate age 5-24"],
    }

    for new_rate, components in rate_calculations.items():
        log.info(f"Calculating.{new_rate}")
        # Only calculating for both sexes
        df_a = df[["country", "year", "value"]][(df["indicator"] == components[0]) & (df["sex"] == "Total")]
        df_a = df_a.rename(columns={"value": components[0]})
        df_b = df[["country", "year", "value"]][(df["indicator"] == components[1]) & (df["sex"] == "Total")]
        df_b = df_b.rename(columns={"value": components[1]})

        df_m = df_a.merge(df_b, on=["country", "year"], how="inner")
        df_m["int_value"] = ((1000 - df_m[components[0]])) / 1000 * df_m[components[1]]
        df_m["indicator"] = new_rate
        df_m["value"] = df_m["int_value"] + df_m[components[1]]
        df_m["unit"] = "Deaths per 1000 live births"
        df_m["sex"] = "Total"
        df_m = df_m[["country", "year", "sex", "indicator", "value", "unit"]]

        output_df.append(df_m)

    out_df = pd.DataFrame(pd.concat(output_df))

    df = pd.concat([df, out_df])
    return df


def calculate_youth_deaths(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate the number of deaths in under fifteens - summing deaths for under fives and for 5-14 year olds"""

    u5_df = df[df["indicator"] == "Under-five deaths"]
    deaths_5_14 = df[df["indicator"] == "Deaths age 5 to 14"]

    u5_df = u5_df[["country", "sex", "unit", "year", "value"]].rename(columns={"value": "Under-five deaths"})
    deaths_5_14 = deaths_5_14[["country", "sex", "unit", "year", "value"]].rename(
        columns={"value": "Deaths age 5 to 14"}
    )

    df_both = u5_df.merge(deaths_5_14, how="inner", on=["country", "sex", "unit", "year"])
    df_both["value"] = df_both["Under-five deaths"] + df_both["Deaths age 5 to 14"]
    df_both["indicator"] = "Under-fifteen deaths"
    df_both["source"] = "OWID based on IGME"
    df_both = df_both.drop(columns=["Under-five deaths", "Deaths age 5 to 14"])
    df = pd.concat([df, df_both])

    return df
