# ETL private

This should not be commited into this repository! It only serves as a showcase and should be moved into its own repo

## Uploading private data to walden

It is possible to upload private data to walden so that only people with S3 credentials would be able to download it.
However, its metadata would still show up in the index file. The way Catalog & Dataset are currently structured makes
it hard to just insert index directory as an argument, so if we ever need private walden, we'd have to restructure it
or come up with workaround.

```
from owid.walden import Dataset

local_file = 'private_test.csv'
metadata = {
  'name': 'private_test',
  'short_name': 'private_test',
  'description': 'testing private walden data',
  'source_name': 'test',
  'url': 'test',
  'license_url': 'test',
  'date_accessed': '2022-02-07',
  'file_extension': 'csv',
  'namespace': '_private_test',
  'publication_year': 2021,
}

# upload the local file to Walden's cache
dataset = Dataset.copy_and_create(local_file, metadata)
# send it as private file to S3
url = dataset.upload(public=False)
# update PUBLIC catalog
dataset.save()
```

## Running private ETL

Run from `etl_private` folder, make sure you have activated the virtual environment for `etl` (with `poetry shell`).

```
etl --dag-path dag-private.yml --force
reindex --catalog ./private_catalog
publish --catalog ./private_catalog --bucket private-owid-catalog
```
