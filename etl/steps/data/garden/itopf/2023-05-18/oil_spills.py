"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger
import numpy as np
from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("oil_spills.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("oil_spills")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["oil_spills"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)

    #
    # Process data.
    #
    log.info("oil_spills.harmonize_countries")
    df = geo.harmonize_countries(
        df=df, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Create a new DataFrame to store the decadal averages
    decadal_averages_df = pd.DataFrame()

    # Iterate over the columns of the original DataFrame
    for column in df.columns:
        # Include only specific columns with values (not country/year)
        if column in ['bel_700t', 'ab_700t', 'oil_spilled']:
            # Calculate the decadal average
            decadal_averages = df.groupby(df['year'] // 10 * 10)[column].mean()
            decadal_averages = decadal_averages.dropna()

            # Place the decadal average values into the corresponding decade columns
            decadal_column = 'decadal_' + str(column)
            decadal_averages_df[decadal_column] = np.round(decadal_averages).astype(int)

    # Merge the original DataFrame with the decadal averages DataFrame based on the 'year' column
    df_decadal = pd.merge(df, decadal_averages_df, on='year', how='outer')

    # Check if the index is unique
    df_decadal.set_index(['country', 'year'], inplace = True)
    assert df_decadal.index.is_unique, "Index is not unique."
    # Reset the index
    df_decadal.reset_index(inplace=True)
    newnames = [name.replace('__', ' ') for name in df_decadal.columns]


    df_decadal.columns = newnames
    df_decadal['country'] = df_decadal['country'].astype(str)

    # Update the 'country' column for specific rows
    df_decadal.loc[df_decadal['country'] == 'La Coruna, Spain', 'country'] = df_decadal.loc[df_decadal['country'] == 'La Coruna, Spain', 'country'] + ', ' + df_decadal.loc[df_decadal['country'] == 'La Coruna, Spain', 'year'].astype(str)



    # Create a new table with the processed data.
    tb_garden = Table(df_decadal, short_name = 'oil_spills')


    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=None)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("oil_spills.end")
