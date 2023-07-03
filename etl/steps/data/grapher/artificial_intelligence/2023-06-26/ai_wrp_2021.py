"""Load a garden dataset and create a grapher dataset."""

from typing import cast

from owid.catalog import Dataset

from etl.helpers import PathFinder, create_dataset, grapher_checks

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = cast(Dataset, paths.load_dependency("ai_wrp_2021"))

    # Read table from garden dataset.
    tb = ds_garden["ai_wrp_2021"]
    # Drop rows with missing values in the 'country' column (used for other ways of grouping things - gender, socio-economic status etc)
    tb = tb[["country", "year", "yes_no_ratio", "help_harm_ratio"]].dropna(subset=["country"]).copy()
    tb.set_index(["country", "year"], inplace=True)

    #
    # Process data.
    #

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb], default_metadata=ds_garden.metadata)

    #
    # Checks.
    #
    grapher_checks(ds_grapher)

    # Save changes in the new grapher dataset.
    ds_grapher.save()