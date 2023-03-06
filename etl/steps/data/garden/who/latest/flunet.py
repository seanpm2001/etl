"""Load a meadow dataset and create a garden dataset."""
import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("flunet.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("flunet")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["flunet"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)

    #
    # Process data.
    #
    log.info("flunet.harmonize_countries")
    df = geo.harmonize_countries(
        df=df, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    df = clean_and_format_data(df)
    df = split_by_surveillance_type(df)
    df = calculate_percent_positive(df)
    # df = create_zero_filled_strain_columns(df)

    # We can't remove sparse data from the zero-filled columns because of how stacked bar charts behave
    # filter_col = [col for col in df if col.endswith("zfilled")]
    # set time-series with less than 10 (non-zero, non-NA) datapoints to NA - apply to a
    # df = remove_sparse_timeseries(df=df, cols=df.columns.drop(["country", "date", filter_col]), min_data_points=10)

    # Create a new table with the processed data.
    # tb_garden = Table(df, like=tb_meadow)
    tb_garden = Table(df, short_name=paths.short_name)
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)
    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("flunet.end")


def remove_rows_that_sum_incorrectly(df: pd.DataFrame) -> pd.DataFrame:
    """
    Let's remove rows that don't add up correctly e.g.
    where influenza a + influenza b does not equal influenza all

    We can't be sure which of the columns are correct and as there are relatively few we should just remove them.
    """
    orig_rows = df.shape[0]
    df = df.drop(df[(df["inf_a"].fillna(0) + df["inf_b"].fillna(0)) != df["inf_all"].fillna(0)].index)
    df = df.drop(
        df[
            (
                df["ah1n12009"].fillna(0)
                + df["ah1"].fillna(0)
                + df["ah3"].fillna(0)
                + df["ah5"].fillna(0)
                + df["ah7n9"].fillna(0)
                + df["anotsubtyped"].fillna(0)
                + df["anotsubtypable"].fillna(0)
                + df["aother_subtype"].fillna(0)
            )
            != df["inf_a"].fillna(0)
        ].index
    )

    df = df.drop(
        df[
            (
                df["bvic_2del"].fillna(0)
                + df["bvic_3del"].fillna(0)
                + df["bvic_nodel"].fillna(0)
                + df["bvic_delunk"].fillna(0)
                + df["byam"].fillna(0)
                + df["bnotdetermined"].fillna(0)
            )
            != df["inf_b"].fillna(0)
        ].index
    )
    new_rows = df.shape[0]
    rows_dropped = orig_rows - new_rows
    log.info(f"{rows_dropped} rows dropped as the disaggregates did not sum correctly")
    assert rows_dropped < 20000, "More than 20,000 rows dropped, this is much more than expected"
    return df


def split_by_surveillance_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pivoting the table so there is a column per variable and per surveillance type

    Summing each column and skipping NAs so there is a column of combined values
    """
    flu_cols = df.columns.drop(["country", "date", "origin_source"])
    df_piv = df.pivot(index=["country", "date"], columns="origin_source").reset_index()

    df_piv.columns = list(map("".join, df_piv.columns))
    sentinel_list = ["SENTINEL", "NONSENTINEL", "NOTDEFINED"]
    for col in flu_cols:
        sum_cols = [col + s for s in sentinel_list]
        df_piv[col + "COMBINED"] = df_piv[sum_cols].sum(axis=1, min_count=1)
    return df_piv


def combine_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Combine columns of:
    * Influenza A with no subtype
    * Influenza B Victoria substrains
    Summing so that NaNs are skipped and not converted to 0
    """
    df["a_no_subtype"] = df[["anotsubtyped", "anotsubtypable", "aother_subtype"]].sum(axis=1, min_count=1)
    df["bvic"] = df[["bvic_2del", "bvic_3del", "bvic_nodel", "bvic_delunk"]].sum(axis=1, min_count=1)

    return df


def create_date_from_iso_week(date_iso: pd.Series) -> pd.Series:
    """
    Convert iso week to date format
    """
    date = pd.to_datetime(date_iso, format="%Y-%m-%d", utc=True).dt.date.astype(str)
    return date


def clean_and_format_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean data by:
    * Converting date to date format
    * Combining subtype columns together
    * Drop unused columns
    """

    df["date"] = create_date_from_iso_week(df["iso_weekstartdate"])
    df = remove_rows_that_sum_incorrectly(df)
    df = combine_columns(df)
    sel_cols = [
        "country",
        "date",
        "origin_source",
        "ah1n12009",
        "ah1",
        "ah3",
        "ah5",
        "ah7n9",
        "a_no_subtype",
        "inf_a",
        "byam",
        "bnotdetermined",
        "bvic",
        "inf_b",
        "inf_all",
        "inf_negative",
        "spec_processed_nb",
        "spec_received_nb",
    ]
    df = df[sel_cols]

    return df


def calculate_percent_positive(df: pd.DataFrame) -> pd.DataFrame:
    """
    Because the data is patchy in some places the WHO recommends three methods for calclating the share of influenza tests that are positive.
    In order of preference
    1. Postive tests divided by positive and negative tests summmed: inf_all/(inf_all + inf_neg)
    2. Positive tests divided by specimens processed: inf_all/spec_processed_nb
    3. Positive tests divided by specimens received: inf_all/spec_received_nb

    Remove rows where the percent is > 100
    Remove rows where the percent = 100 but all available denominators are 0.
    """
    surveillance_cols = ["SENTINEL", "NONSENTINEL", "NOTDEFINED", "COMBINED"]

    for col in surveillance_cols:
        df["pcnt_pos_1" + col] = (df["inf_all" + col] / (df["inf_all" + col] + df["inf_negative" + col])) * 100
        df["pcnt_pos_2" + col] = (df["inf_all" + col] / df["spec_processed_nb" + col]) * 100
        df["pcnt_pos_3" + col] = (df["inf_all" + col] / df["spec_received_nb" + col]) * 100

        # hierachically fill the 'pcnt_pos' column with values from the columns described above in order of preference: 1->2->3
        df["pcnt_pos" + col] = df["pcnt_pos_1" + col]
        df["pcnt_pos" + col] = df["pcnt_pos" + col].fillna(df["pcnt_pos_2" + col])
        df["pcnt_pos" + col] = df["pcnt_pos" + col].fillna(df["pcnt_pos_3" + col])

        df = df.drop(columns=["pcnt_pos_1" + col, "pcnt_pos_2" + col, "pcnt_pos_3" + col])

        # Drop rows where pcnt_pos is >100
        df.loc[df["pcnt_pos" + col] > 100, "pcnt_pos" + col] = np.nan

        # Rows where the percentage positive is 100 but all possible denominators are 0
        df.loc[
            (df["pcnt_pos" + col] == 100)
            & (df["inf_negative" + col] == 0)
            & (df["spec_processed_nb" + col] == 0)
            & (df["spec_received_nb" + col] == 0),
            "pcnt_pos" + col,
        ] = np.nan
        df = df.dropna(axis=1, how="all")

    return df


# remove data for countries that have less than 5 or 10 data points

# def sanity_checks(df: pd.DataFrame) -> pd.DataFrame:
#    """
#    Some assertions to check that the variables are as expected e.g. all the of the influenza strains sum to the influenza all column.
#    """
##
#
#    assert all(
#        df[
#            [
##                "ah1n12009NONSENTINEL",
#                "ah1NONSENTINEL",
#                "ah3NONSENTINEL",
#                "ah5NONSENTINEL",
#                "ah7n9NONSENTINEL",
#                "a_no_subtypeNONSENTINEL",
##            ]
#        ].sum(axis=1, min_count=1)
#        == df["inf_aNONSENTINEL"]