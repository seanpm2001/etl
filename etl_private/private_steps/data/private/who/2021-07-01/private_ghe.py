import pandas as pd
from owid.catalog import Dataset
from owid import walden, catalog
from etl.steps.data.converters import convert_walden_metadata


def run(dest_dir: str) -> None:
    # Load private dataset
    private_dataset = walden.Catalog().find_one("_private_test", "2021", "private_test")
    df = pd.read_csv(private_dataset.ensure_downloaded())

    t = catalog.Table(df)
    t.metadata = catalog.TableMeta(
        short_name="private_test",
    )

    # Create dataset
    print(f"Saving to {dest_dir}")
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(private_dataset)

    ds.add(t)
    ds.save()
