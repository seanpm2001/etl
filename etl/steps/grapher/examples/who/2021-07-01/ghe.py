from owid import catalog
from collections.abc import Iterable
from pathlib import Path

from etl.paths import DATA_DIR
from etl import grapher_helpers as gh


annotations_path = Path(__file__).parent / "annotations.yml"


def get_grapher_dataset() -> catalog.Dataset:
    annot = gh.Annotation.load_from_yaml(annotations_path)
    return annot.create_dataset()


def get_grapher_tables(dataset: catalog.Dataset) -> Iterable[catalog.Table]:
    annot = gh.Annotation.load_from_yaml(annotations_path)

    table = dataset["estimates"]

    # Get the legacy_entity_id from the country_code via the countries_regions dimension table
    reference_dataset = catalog.Dataset(DATA_DIR / "reference")
    countries_regions = reference_dataset["countries_regions"]
    table = table.merge(
        right=countries_regions[["legacy_entity_id"]],
        how="left",
        left_on="country_code",
        right_index=True,
        validate="m:1",
    )

    # Add entity_id and year
    table.reset_index(inplace=True)
    table["year"] = table["year"].astype(int)
    table["entity_id"] = table["legacy_entity_id"].astype(int)

    gh.validate_table(annot, table)

    yield from gh.yield_table(annot, table, metadata=table.metadata)
