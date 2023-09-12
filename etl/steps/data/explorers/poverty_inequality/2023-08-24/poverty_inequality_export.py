"""Load a garden dataset and create an explorers dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset for LIS and WID
    ds_lis = paths.load_dataset("luxembourg_income_study")
    ds_wid = paths.load_dataset("world_inequality_database")

    # Read table from garden dataset.
    tb_lis = ds_lis["luxembourg_income_study"].reset_index()
    tb_wid = ds_wid["world_inequality_database"].reset_index()

    tb_lis_percentiles = ds_lis["lis_percentiles"].reset_index()
    tb_wid_percentiles = ds_wid["world_inequality_database_distribution"].reset_index()

    # LIS PERCENTILES

    # Make lis table longer
    tb_lis_percentiles = tb_lis_percentiles.melt(
        id_vars=["country", "year", "welfare", "equivalization", "percentile"],
        value_vars=["thr", "share"],
        var_name="indicator_name",
        value_name="value",
    )

    # Reduce percentile column by 1 when variable is share (when it's not thr in the code)
    tb_lis_percentiles["percentile"] = tb_lis_percentiles["percentile"].where(
        tb_lis_percentiles["indicator_name"] == "thr", tb_lis_percentiles["percentile"] - 1
    )

    # Replace percentile 100 with 0 (it's always null and only for thr)
    tb_lis_percentiles["percentile"] = tb_lis_percentiles["percentile"].replace(100, 0)

    # Sort by country, year, welfare, equivalization, percentile and variable
    tb_lis_percentiles = tb_lis_percentiles.sort_values(
        ["country", "year", "welfare", "equivalization", "indicator_name", "percentile"]
    )

    # Create WID nomenclature for percentiles
    tb_lis_percentiles["percentile"] = (
        "p" + tb_lis_percentiles["percentile"].astype(str) + "p" + (tb_lis_percentiles["percentile"] + 1).astype(str)
    )

    # Filter out welfare dhci
    tb_lis_percentiles = tb_lis_percentiles[tb_lis_percentiles["welfare"] != "dhci"].reset_index(drop=True)

    # Rename welfare, equivalization and indicator_name columns
    tb_lis_percentiles["welfare"] = tb_lis_percentiles["welfare"].replace({"mi": "market", "dhi": "disposable"})
    tb_lis_percentiles["equivalization"] = tb_lis_percentiles["equivalization"].replace(
        {"eq": "equivalized", "pc": "perCapita"}
    )
    tb_lis_percentiles["indicator_name"] = tb_lis_percentiles["indicator_name"].replace(
        {"thr": "threshold"}
    )  # NOTE: Don't forger avg when I add it

    # Add the column series_code, which is the concatenation of welfare, equivalization and indicator_name
    tb_lis_percentiles["series_code"] = (
        "lis_"
        + tb_lis_percentiles["welfare"].astype(str)
        + "_"
        + tb_lis_percentiles["equivalization"].astype(str)
        + "_2017ppp2017"
    )

    # When indicator_name is share, remove the last values after the "_"
    tb_lis_percentiles["series_code"] = tb_lis_percentiles["series_code"].where(
        tb_lis_percentiles["indicator_name"] != "share",
        tb_lis_percentiles["series_code"].str.replace("_2017ppp2017", ""),
    )

    # Remove columns welfare and equivalization
    tb_lis_percentiles = tb_lis_percentiles.drop(columns=["welfare", "equivalization"])

    # WID PERCENTILES

    # Make wid table longer
    tb_wid_percentiles = tb_wid_percentiles.melt(
        id_vars=["country", "year", "welfare", "p", "percentile"],
        value_vars=["thr", "avg", "share", "thr_extrapolated", "avg_extrapolated", "share_extrapolated"],
        var_name="indicator_name",
        value_name="value",
    )

    # Sort by country, year, welfare, equivalization, percentile and variable
    tb_wid_percentiles = tb_wid_percentiles.sort_values(["country", "year", "welfare", "indicator_name", "p"])

    # Select only welfare types needed
    tb_wid_percentiles = tb_wid_percentiles[tb_wid_percentiles["welfare"].isin(["pretax", "posttax_nat"])].reset_index(
        drop=True
    )

    # Rename welfare and indicator_name columns
    tb_wid_percentiles["welfare"] = tb_wid_percentiles["welfare"].replace(
        {"pretax": "pretaxNational", "posttax_nat": "posttaxNational"}
    )

    # In indicator_name, replace values that contain avg with average and thr with threshold
    tb_wid_percentiles["indicator_name"] = tb_wid_percentiles["indicator_name"].replace(
        {
            "avg": "average",
            "thr": "threshold",
            "avg_extrapolated": "average_extrapolated",
            "thr_extrapolated": "threshold_extrapolated",
        }
    )

    # Drop percentile values containing "."
    tb_wid_percentiles = tb_wid_percentiles[~tb_wid_percentiles["percentile"].str.contains("\\.")].reset_index(
        drop=True
    )

    # Add the column series_code, which is the concatenation of welfare, equivalization and indicator_name
    tb_wid_percentiles["series_code"] = (
        "wid_" + tb_wid_percentiles["welfare"].astype(str) + "_perAdult" + "_2011ppp2022"
    )

    # When indicator_name is share or share_extrapolated, remove the last values after the "_"
    tb_wid_percentiles["series_code"] = tb_wid_percentiles["series_code"].where(
        ~tb_wid_percentiles["indicator_name"].str.contains("share"),
        tb_wid_percentiles["series_code"].str.replace("_2011ppp2022", ""),
    )

    # Remove columns welfare and percentile
    tb_wid_percentiles = tb_wid_percentiles.drop(columns=["welfare", "p"])

    # Create two different tables, one for extrapolated
    tb_wid_percentiles_extrapolated = tb_wid_percentiles[
        tb_wid_percentiles["indicator_name"].str.contains("extrapolated")
    ].reset_index(drop=True)
    tb_wid_percentiles = tb_wid_percentiles[
        ~tb_wid_percentiles["indicator_name"].str.contains("extrapolated")
    ].reset_index(drop=True)

    # Concatenate all the tables
    tb = pr.concat(
        [tb_lis_percentiles, tb_wid_percentiles, tb_wid_percentiles_extrapolated],
        short_name="poverty_inequality_export",
    )

    # Remove null values in the value column
    tb = tb.dropna(subset=["value"])

    # Set index
    tb = tb.set_index(["country", "year", "series_code", "percentile", "indicator_name"], verify_integrity=True)
    tb_lis_percentiles = tb_lis_percentiles.set_index(
        ["country", "year", "series_code", "percentile", "indicator_name"], verify_integrity=True
    )
    tb_wid_percentiles = tb_wid_percentiles.set_index(
        ["country", "year", "series_code", "percentile", "indicator_name"], verify_integrity=True
    )
    tb_wid_percentiles_extrapolated = tb_wid_percentiles_extrapolated.set_index(
        ["country", "year", "series_code", "percentile", "indicator_name"], verify_integrity=True
    )

    # Set a different short_name fot extrapolated table
    tb_wid_percentiles_extrapolated.metadata.short_name = "world_inequality_database_distribution_extrapolated"

    # Create explorer dataset, with garden table and metadata in csv format
    ds_explorer = create_dataset(
        dest_dir,
        tables=[tb, tb_lis_percentiles, tb_wid_percentiles, tb_wid_percentiles_extrapolated],
        formats=["csv"],
        default_metadata=ds_lis.metadata,
    )
    ds_explorer.save()
