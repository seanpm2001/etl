import pathlib

import click

from etl.snapshot import Snapshot

CURRENT_DIR = pathlib.Path(__file__).parent


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Walden",
)
def main(upload: bool) -> None:
    snap = Snapshot(CURRENT_DIR / "baseline.zip")
    snap.download_from_source()
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
