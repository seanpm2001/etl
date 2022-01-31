import pandas as pd
from owid import catalog
from collections.abc import Iterable
import yaml
import warnings
import slugify

from etl.paths import DATA_DIR

from typing import Optional, Dict
from pydantic import BaseModel


class DatasetModel(BaseModel):
    source: str
    short_name: str
    namespace: str


class DimensionModel(BaseModel):
    pass


class VariableModel(BaseModel):
    description: str
    unit: str
    short_unit: Optional[str]


class Annotation(BaseModel):
    dataset: DatasetModel
    dimensions: Dict[str, Optional[DimensionModel]]
    variables: Dict[str, VariableModel]

    @property
    def dimension_names(self):
        return list(self.dimensions.keys())

    @property
    def variable_names(self):
        return list(self.variables.keys())

    @classmethod
    def load_from_yaml(cls, path):
        # Load variable descriptions and units from the annotations.yml file and
        # store them as column metadata
        with open(path) as istream:
            annotations = yaml.safe_load(istream)
        return cls.parse_obj(annotations)

    def create_dataset(self) -> catalog.Dataset:
        # TODO: we already have this dataset path... can we reuse it?
        dataset = catalog.Dataset(DATA_DIR / self.dataset.source)
        dataset.metadata.short_name = self.dataset.short_name
        dataset.metadata.namespace = self.dataset.namespace
        return dataset


def yield_table(
    annot: Annotation, table: pd.DataFrame, metadata: catalog.TableMeta
) -> Iterable[catalog.Table]:
    # We have 5 dimensions but graphers data model can only handle 2 (year and entityId). This means
    # we have to iterate all combinations of the remaining 3 dimensions and create a new variable for
    # every combination that cuts out only the data points for a specific combination of these 3 dimensions
    # Grapher can only handle 2 dimensions (year and entityId)
    table = catalog.Table(table, metadata=metadata)

    # Load variable descriptions and units from the annotations.yml file and
    # store them as column metadata
    for column in annot.variable_names:
        v = annot.variables[column]
        if column not in table:
            # TODO: logging.warning would be better
            warnings.warn("Column {} not in table".format(column))
        else:
            table[column].metadata.description = v.description
            table[column].metadata.unit = v.unit
            table[column].metadata.short_unit = v.short_unit

    for dims, table_to_yield in table.groupby(annot.dimension_names):
        print(" - ".join(dims))

        # Now iterate over every column in the original dataset and export the
        # subset of data that we prepared above
        for column in set(annot.variable_names) & set(table.columns):

            # Add column and dimensions as short_name
            table_to_yield.metadata.short_name = slugify.slugify(
                "-".join([column] + list(dims))
            )

            # Safety check to see if the metadata is still intact
            assert (
                table_to_yield[column].metadata.unit is not None
            ), "Unit should not be None here!"

            yield table_to_yield.set_index(["year", "entity_id"])[[column]]


def validate_table(annot: Annotation, table: pd.DataFrame):
    # Since this script expects a certain structure make sure it is actually met
    missing_cols = set(annot.dimension_names + ["year", "entity_id"]) - set(
        table.columns
    )
    if missing_cols:
        raise Exception(f"Table is missing required columns {missing_cols}")

    missing_cols = set(annot.variable_names) - set(table.columns)
    if missing_cols:
        raise Exception(f"Table is missing required columns {missing_cols}")
