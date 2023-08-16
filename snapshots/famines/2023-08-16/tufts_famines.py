"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd
import requests
from bs4 import BeautifulSoup
from owid.datautils.io import df_to_file

from etl.snapshot import Snapshot

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
    # Create a new snapshot.
    snap = Snapshot(f"famines/{SNAPSHOT_VERSION}/tufts_famines.csv")
    # Get the HTML content from the URL
    response = requests.get("https://sites.tufts.edu/wpf/famine/")
    html_content = response.content

    # Parse the HTML with BeautifulSoup
    soup = BeautifulSoup(html_content, "html.parser")

    # Search for the table using a part of the provided data (this should help to identify the correct table)
    target_table = None
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all("td")
            # Check if a known piece of data is present in the current row
            if cells and "1870-71" in cells[0].get_text() and "Persia" in cells[1].get_text():
                target_table = table
                break
        if target_table:
            break

    # Extract data from the identified table and store it in a list
    rows_data = []
    for row in target_table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) == 5:  # Ensure we only capture rows with the expected number of cells
            row_content = [cell.get_text(strip=True) for cell in cells]
            rows_data.append(row_content)

    # Convert the list to a pandas DataFrame
    df = pd.DataFrame(rows_data, columns=["Years", "Country", "Cause", "Deaths", "Source"])
    df = df.drop(df.index[0])

    # Save the merged dataset to the snapshot path
    df_to_file(df, file_path=snap.path)

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
