from owid import catalog
from collections.abc import Iterable
from pathlib import Path

from etl.paths import DATA_DIR
from etl import grapher_helpers as gh


annotations_path = Path(__file__).parent / "annotations.yml"


# TODO: this function could be called by default when none is present
def get_grapher_dataset() -> catalog.Dataset:
    annot = gh.Annotation.load_from_yaml(annotations_path)
    return annot.create_dataset()


def get_grapher_tables(dataset: catalog.Dataset) -> Iterable[catalog.Table]:
    annot = gh.Annotation.load_from_yaml(annotations_path)

    table = dataset["math_skills"]

    # Get the legacy_entity_id from the country_code via the countries_regions dimension table
    reference_dataset = catalog.Dataset(DATA_DIR / "reference")
    countries_regions = reference_dataset["countries_regions"]
    table = table.merge(
        right=countries_regions[["name", "legacy_entity_id"]],
        how="left",
        left_on="GeoAreaName",
        right_on="name",
        validate="m:1",
    )

    # TODO: contains null values after joining! do something with them
    table.dropna(subset=["legacy_entity_id"], inplace=True)

    # Add entity_id and year
    table["year"] = table["TimePeriod"].astype(int)
    table["entity_id"] = table["legacy_entity_id"].astype(int)

    # Keep only selected indicators
    table = table.loc[table.SeriesCode.isin({"SE_TOT_GPI", "SE_TOT_PRFL"})]

    # TODO: Would be nice to have a support for both long and wide tables in `yield_table`
    for series_code in {"SE_TOT_GPI", "SE_TOT_PRFL"}:
        table_s = table.loc[table.SeriesCode == series_code]

        table_s = table_s.rename(
            columns={
                "[Education level]": "education_level",
                "[Sex]": "sex",
                "Value": series_code.lower(),
            }
        )

        yield from gh.yield_table(
            annot, table_s, metadata=dataset["math_skills"].metadata
        )
