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
    log.info("co2_air_transport.start")
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("co2_air_transport")
    ds_tour: Dataset = paths.load_dependency("unwto")
    ds_flights_2011_2021: Dataset = paths.load_dependency("eu_flights_2019_2021")
    ds_flights_2022: Dataset = paths.load_dependency("eu_flights_2022")


    # Read table from meadow dataset.
    tb_meadow = ds_meadow["co2_air_transport"]
    tb_tour = ds_tour["unwto"]

    tb_flights_19_21 = ds_flights_2011_2021['eu_flights_2019_2021']
    tb_flights_22 =  ds_flights_2022['eu_flights_2022']

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)
    df_tr = pd.DataFrame(tb_tour)
    df_19_21 = pd.DataFrame(tb_flights_19_21)
    df_22 = pd.DataFrame(tb_flights_22)

    concatenated_flights = pd.concat([df_19_21, df_22])
    date_column = pd.to_datetime(concatenated_flights['year'].astype(str) + '-' + concatenated_flights['month'].astype(str) + '-15')

    # Assign the 'date' column to the DataFrame
    concatenated_flights['date'] = date_column
    concatenated_flights['days_since_2019'] = (concatenated_flights['date'] - pd.to_datetime('2019-01-01')).dt.days
    concatenated_flights.drop(['month','year', 'date'], axis = 1, inplace = True)
    concatenated_flights.rename(columns={'days_since_2019': 'year'}, inplace=True)
    concatenated_flights = concatenated_flights.groupby(['year', 'country']).sum(numeric_only = True)
    concatenated_flights.reset_index(inplace = True)

    regions_ = ["European Union (27)"]

    for region in regions_:
         concatenated_flights = geo.add_region_aggregates(df = concatenated_flights, country_col='country', year_col='year', region=region)

    europe_rows = concatenated_flights[concatenated_flights['country'] == 'European Union (27)']
    #
    # Process data.
    #
    log.info("co2_air_transport.harmonize_countries")
    df = geo.harmonize_countries(
        df=df, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    df = df[df['flight_type'] == 'P']
    df.drop('flight_type', axis = 1, inplace = True)
    df = df[df['emission_source'].isin(['TER_DOM', 'TER_INT'])]
    df = df.reset_index(drop=True)

    df_an = df[df['frequency'] == 'Annual']
    df_an = df_an.drop(['frequency', 'month'], axis = 1)


    df_an['emission_source'] = df_an['emission_source'].apply(lambda x: x + '_a')
    df_an.set_index(['country', 'year', 'emission_source'], inplace = True)

    assert df_an.index.is_unique, "Index is not well constructed"
    df_an.reset_index(inplace = True)

    pivot_table_ye = pd.pivot_table(df_an, values='value', index=['country', 'year'], columns=['emission_source'])
    pivot_table_ye.reset_index(inplace = True)
    # Add population data to the DataFrame
    pivot_table_ye = geo.add_population_to_dataframe(pivot_table_ye, country_col="country", year_col="year", population_col="population")
    emissions_columns = pivot_table_ye.columns[2:-1]

    for col in emissions_columns:
        pivot_table_ye[f'per_capita_{col}'] = pivot_table_ye[col] / pivot_table_ye['population']
    just_inb_ratio = df_tr[['country', 'year','inb_outb']]
    pivot_outb = pd.merge(pivot_table_ye, just_inb_ratio, on = ['year', 'country'])
    pivot_outb = pd.merge(pivot_table_ye, just_inb_ratio, on = ['year', 'country'])
    pivot_outb['int_inb_out'] = pivot_outb['TER_INT_a']*pivot_outb['inb_outb']
    pivot_outb = pivot_outb.drop(['inb_outb'], axis = 1)


    df_mn = df[df['frequency'] == 'Monthly']
    df_mn = df_mn.drop(['frequency'], axis = 1)
    # Create a new 'date' column separately
    date_column = pd.to_datetime(df_mn['year'].astype(str) + '-' + df_mn['month'].astype(str) + '-15')

    # Assign the 'date' column to the DataFrame
    df_mn['date'] = date_column
    df_mn['emission_source'] = df_mn['emission_source'].apply(lambda x: x + '_m')

    # Split by month
    pivot_df = pd.pivot_table(df_mn[df_mn['emission_source']=='TER_INT_m'], values='value', index=['country', 'year'], columns='month')
    # Define a list of month names
    pivot_df.reset_index(inplace = True)
    month_names = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
    # Rename the columns with month names
    pivot_df.columns = ['country', 'year'] + month_names

    df_mn['days_since_2019'] = (df_mn['date'] - pd.to_datetime('2019-01-01')).dt.days
    df_mn.drop(['month','year', 'date'], axis = 1, inplace = True)

    df_mn.rename(columns={'days_since_2019': 'year'}, inplace=True)

    pivot_table_mn = pd.pivot_table(df_mn, values = 'value', index = ['country', 'year'], columns = ['emission_source'])

    pivot_table_mn.reset_index(inplace = True)
    concatenated_df = pd.merge(pivot_outb, pivot_table_mn,on = ['year', 'country'], how = 'outer')
    merge_df = pd.merge(concatenated_df, pivot_df, on = ['year', 'country'], how = 'outer')
    merge_df = ukraine_fill_war_for_reg_agg(merge_df)
    merge_df = merge_df.drop('TER_INT_m', axis = 1)

    merge_df = merge_df.rename(columns={'ter_int_m_filled_ukraine': 'TER_INT_m'})

    regions_ = ["North America",
        "South America",
        "Europe",
        "Africa",
        "Asia",
        "Oceania",
        "European Union (27)"]

    for region in regions_:
         merge_df = geo.add_region_aggregates(df = merge_df, country_col = 'country', year_col = 'year', region = region)

    merge_df['TER_INT_m'] = merge_df['TER_INT_m'].replace(0, np.nan)
    with_flights = pd.merge(merge_df,europe_rows, on = ['year', 'country'], how = 'outer')
    with_flights.loc[with_flights['country'] == "European Union (27)", 'TER_INT_m_EU27'] = with_flights['TER_INT_m']
    with_flights.rename(columns = {'flights': 'flights_eu'}, inplace = True)

    with_flights = with_flights[with_flights['year'] != 2023]

    month_names = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']

    def update_values(row):
        country = row['country']
        if country in regions_[:-1]:
            return row
        else:
            row[month_names] = float('nan')  # Set values to NaN for month columns starting from the 3rd column
            return row

    # Apply the function to each row using apply()
    with_flights = with_flights.apply(update_values, axis=1)
    with_flights.reset_index(inplace = True)
    with_flights.drop('index', axis = 1, inplace = True)
    # Melt the DataFrame to have months as rows

    month_names = ['country','year','January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
    df_extracted = with_flights[month_names].copy()
    df_melted = df_extracted.melt(id_vars=['country', 'year'], var_name='month', value_name='value')
    filtered_df = df_melted[df_melted['value'].notnull()]
    # Pivot the melted DataFrame to reshape it with countries as columns
    df_pivoted = filtered_df.pivot(index=['month','year'], columns=['country'], values='value')
    df_pivoted.reset_index(inplace = True)
    df_pivoted = df_pivoted.rename(columns={'month': 'country'})
    mrg = pd.merge(df_pivoted,with_flights, on = ['country','year'], how = 'outer')

    # Create a new table with the processed data.
    tb_garden = Table(mrg, short_name = 'co2_air_transport')

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=None)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("co2_air_transport.end")


def ukraine_fill_war_for_reg_agg(df):
    # Get a list of unique countries
    unique_countries = df['country'].unique()

    # Get the range of years that exist in the 'year' column
    all_years = df['year'].unique() # Inclusive range of years

    # Create a DataFrame with all possible combinations of years and countries
    new_index = pd.MultiIndex.from_product([all_years, unique_countries], names=['year', 'country'])
    new_df = pd.DataFrame(index=new_index).reset_index()

    # Merge the new DataFrame with the original DataFrame
    merged_df = pd.merge(new_df, df, on=['year', 'country'], how='left')

    # Optionally, sort the resulting DataFrame by the 'year' and 'country' columns
    merged_df = merged_df.sort_values(['year', 'country']).reset_index(drop=True)

    # Set values to NaN for missing years
    merged_df.loc[merged_df['year'] < merged_df['year'].min(), 'TER_INT_m'] = float('nan')
    merged_df.loc[merged_df['year'] > merged_df['year'].max(), 'TER_INT_m'] = float('nan')

    # Create a Boolean mask to identify rows where the year is outside the desired range
    year_mask = (merged_df['year'] < 2015) | (merged_df['year'] > 2023)

    # Create a Boolean mask to identify rows where the country is 'Ukraine'
    country_mask = merged_df['country'] == 'Ukraine'

    # Combine the masks using the logical AND operator
    combined_mask = year_mask & country_mask

    # Create the 'country_filled' column and fill it with the original 'country' values
    merged_df['ter_int_m_filled_ukraine'] = merged_df['TER_INT_m']

    # Fill NaN values with 0s in the 'country_filled' column where the year is outside the range and the country is 'Ukraine'
    merged_df.loc[combined_mask, 'ter_int_m_filled_ukraine'] = merged_df.loc[combined_mask, 'ter_int_m_filled_ukraine'].fillna(0)

    merged_df[['year','ter_int_m_filled_ukraine']][country_mask]

    return merged_df