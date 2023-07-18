"""Load a meadow dataset and create a garden dataset."""

from typing import List, cast

import pandas as pd
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("education_lee_lee"))

    # Read table from meadow dataset.
    tb = ds_meadow["education_lee_lee"]
    tb.reset_index(inplace=True)

    #
    # Process data.
    #
    tb: Table = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb["age_group"] = tb["age_group"].replace(
        {
            "15.0-64.0": "Youth and Adults (15-64 years)",
            "15.0-24.0": "Youth (15-24 years)",
            "25.0-64.0": "Adults (25-64 years)",
            "not specified": "Age not specified",
        }
    )
    # Clean and pivot enrollment data by sex
    df_enrollment = prepare_enrollment_data(tb)
    # Clean and pivot attainment data by sex and age
    df_attainment = prepare_attainment_data(tb)

    merged_df = pd.merge(df_enrollment, df_attainment, on=["year", "country", "region"], how="outer")
    tb = Table(merged_df, short_name=paths.short_name, underscore=True)
    tb.set_index(["country", "year"], inplace=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def melt_and_pivot(
    df: pd.DataFrame, id_vars: List[str], value_vars: List[str], index_vars: List[str], columns_vars: List[str]
) -> pd.DataFrame:
    """
    Melt the dataframe to long format and then pivot it to wide format.

    Parameters:
    df (DataFrame): Original dataframe to process.
    id_vars (List[str]): Identifier variables for melting.
    value_vars (List[str]): Column(s) to unpivot.
    index_vars (List[str]): Column(s) to set as index for pivoting.
    columns_vars (List[str]): Column(s) to pivot on.

    Returns:
    DataFrame: Processed dataframe.
    """
    df_melted = df.melt(id_vars=id_vars, value_vars=value_vars, var_name="measurement", value_name="value")
    df_pivot = df_melted.pivot(index=index_vars, columns=columns_vars, values="value")

    return df_pivot


def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flatten hierarchical columns in dataframe.

    Parameters:
    df (DataFrame): Dataframe with multi-level columns.

    Returns:
    DataFrame: Dataframe with flattened columns.
    """
    df.columns = ["_".join(col).strip() for col in df.columns.values]
    df = df.reset_index()

    return df


def prepare_enrollment_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare the dataframe by selecting required columns and drop the rows
    where all values in the specified columns are NaN.

    Parameters:
    df (DataFrame): Original dataframe to process.

    Returns:
    DataFrame: Processed dataframe with enrollment data.
    """
    id_vars_enrollment = ["country", "year", "sex", "region"]
    enrollment_columns = ["primary_enrollment_rates", "secondary_enrollment_rates", "tertiary_enrollment_rates"]
    df_enrollment = df[enrollment_columns + id_vars_enrollment]
    df_enrollment = df_enrollment.dropna(subset=enrollment_columns, how="all")
    df_enrollment.reset_index(drop=True, inplace=True)

    df_pivot_enrollment = melt_and_pivot(
        df_enrollment, id_vars_enrollment, enrollment_columns, ["country", "year", "region"], ["sex", "measurement"]
    )
    df_pivot_enrollment = flatten_columns(df_pivot_enrollment)

    return df_pivot_enrollment


def prepare_attainment_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare the dataframe by selecting required columns.

    Parameters:
    df (DataFrame): Original dataframe to process.

    Returns:
    DataFrame: Processed dataframe with attainment data.
    """
    id_vars_attainment = ["country", "year", "sex", "age_group", "region"]
    attainment_columns = [
        "percentage_of_no_education",
        "percentage_of_primary_education",
        "percentage_of_complete_primary_education_attained",
        "percentage_of_secondary_education",
        "percentage_of_complete_secondary_education_attained",
        "percentage_of_tertiary_education",
        "percentage_of_complete_tertiary_education_attained",
        "average_years_of_education",
        "average_years_of_primary_education",
        "average_years_of_secondary_education",
        "average_years_of_tertiary_education",
        "population__thousands",
    ]

    df_pivot = melt_and_pivot(
        df[id_vars_attainment + attainment_columns],
        id_vars_attainment,
        attainment_columns,
        ["country", "year", "region"],
        ["sex", "age_group", "measurement"],
    )
    df_pivot = flatten_columns(df_pivot)

    cols_to_drop = [col for col in df_pivot.columns if "Age not specified" in col]
    df_pivot = df_pivot.drop(columns=cols_to_drop)

    return df_pivot