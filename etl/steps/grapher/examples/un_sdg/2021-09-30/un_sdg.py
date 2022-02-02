from owid import catalog
from collections.abc import Iterable
from pathlib import Path

from etl.paths import DATA_DIR
from etl import grapher_helpers as gh


annotations_path = Path(__file__).parent / "annotations.yml"


def get_grapher_dataset() -> catalog.Dataset:
    dataset = catalog.Dataset(DATA_DIR / 'examples/un_sdg/2021-09-30/un_sdg')
    dataset.metadata.short_name = 'un_sdg-2021-09-30'
    dataset.metadata.namespace = 'examples'
    return dataset


def get_grapher_tables(dataset: catalog.Dataset) -> Iterable[catalog.Table]:
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

    dimensions = [
        "entity_id",
        "year",
        "education_level",
        "sex",
    ]

    # TODO: Would be nice to have a support for both long and wide tables in `yield_table`
    for series_code in {"SE_TOT_GPI", "SE_TOT_PRFL"}:
        t = table.loc[table.SeriesCode == series_code]

        t = t.rename(
            columns={
                "[Education level]": "education_level",
                "[Sex]": "sex",
                "Value": series_code.lower(),
            }
        )

        t = t.set_index(
            dimensions,
        )[[series_code.lower()]]

        t = gh.as_table(t, dataset["math_skills"])
        t = gh.annotate_table_from_yaml(t, annotations_path, missing_col='ignore')

        yield from gh.yield_table(t)
