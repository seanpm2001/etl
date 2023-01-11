import json
import tempfile
import zipfile
from os import listdir
from os.path import isfile, join
from typing import List, cast

import geopandas as gpd
import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import Names
from etl.paths import DATA_DIR
from etl.snapshot import Snapshot

log = get_logger()
# naming conventions
N = Names(__file__)


def run(dest_dir: str) -> None:
    log.info("country_extract.start")

    # retrieve snapshot of basemap
    snap_map = Snapshot("basemap/2023-01-11/grapher_basemap.topo.json")

    gdf = gpd.GeoDataFrame.from_file(snap_map.path)

    # retrieve snapshot of hyde
    snap_hyde = Snapshot("hyde/2017/general_files.zip")
    with tempfile.TemporaryDirectory() as tmpdirname:
        with zipfile.ZipFile(snap_hyde.path, "r") as zip_ref:
            zip_ref.extractall("~")
        onlyfiles = listdir(tmpdirname)
        print(onlyfiles)

    # read dataset from meadow
    ds_meadow = Dataset(DATA_DIR / "meadow/hyde/2023-11-01/country_extract")
    tb_meadow = ds_meadow["country_extract"]

    df = pd.DataFrame(tb_meadow)

    log.info("country_extract.exclude_countries")
    df = exclude_countries(df)

    log.info("country_extract.harmonize_countries")
    df = harmonize_countries(df)

    # create new dataset with the same metadata as meadow
    ds_garden = Dataset.create_empty(dest_dir, metadata=ds_meadow.metadata)

    # create new table with the same metadata as meadow and add it to dataset
    tb_garden = Table(df, like=tb_meadow)
    ds_garden.add(tb_garden)

    # update metadata from yaml file
    ds_garden.update_metadata(N.metadata_path)

    ds_garden.save()

    log.info("country_extract.end")


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
