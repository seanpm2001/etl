"""Script to create a snapshot of dataset 'CO2 emissions from commercial aviation (ICCT, 2020)'."""

from pathlib import Path

import click

from etl.snapshot import Snapshot
import requests
# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)




def main(upload: bool) -> None:
    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"
}

    res = requests.get('https://theicct.org/wp-content/uploads/2022/01/GACA-2020-Public-Data-Sheet.xlsx', headers=headers)

    # Create a new snapshot.
    snap = Snapshot(f"tourism/{SNAPSHOT_VERSION}/icct_co2.xlsx")
    snap.metadata.source_data_url = res.url
    # Download data from source.
    snap.download_from_source()

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
