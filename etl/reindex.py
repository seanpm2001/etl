#
#  reindex.py
#  etl
#

import click
from pathlib import Path

from owid.catalog import LocalCatalog

from etl.paths import DATA_DIR


@click.command()
@click.option("--catalog", type=click.Path(exists=True), help="Path to catalog", default=Path(DATA_DIR))
def reindex(catalog: Path) -> None:
    LocalCatalog(catalog).reindex()


if __name__ == "__main__":
    reindex()
