import json

import geopandas as gpd
import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import Names
from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata

log = get_logger()

# naming conventions
N = Names(__file__)


def run(dest_dir: str) -> None:

    # create new dataset and reuse walden metadata
    ds = Dataset.create_empty(dest_dir, metadata=convert_snapshot_metadata(snap.metadata))
    ds.metadata.version = "2023-01-11"

    # # create table with metadata from dataframe and underscore all columns
    tb = Table(gdf, short_name=snap.metadata.short_name, underscore=True)

    # add table to a dataset
    ds.add(tb)

    # update metadata
    ds.update_metadata(N.metadata_path)

    # finally save the dataset
    ds.save()

    log.info("grapher_basemap.end")
