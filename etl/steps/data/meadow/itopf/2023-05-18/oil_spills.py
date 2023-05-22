"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

import urllib.request
import pdfplumber
from io import BytesIO
import re
# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("oil_spills.start")
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap: Snapshot = paths.load_dependency("oil_spills.pdf")

    # Process data.
    # Load data from url instead of snapshot bc it's a pdf file.
    url_to_data = 'https://www.itopf.org/fileadmin/uploads/itopf/data/Photos/Statistics/Oil_Spill_Stats_brochure_2022.pdf'
    pdf_content = extract_pdf_content(url_to_data)

    with pdfplumber.open(BytesIO(pdf_content)) as pdf:
        num_pages = len(pdf.pages)
        for page_number in range(num_pages):
            if page_number == 6:  # extract oil spilled
                text = extract_text_from_page(pdf, page_number)
                df_oil_spilled = extract_spill_quantity(text)

                assert df_oil_spilled.index.is_unique
                df_oil_spilled.reset_index(inplace = True)

            elif page_number == 5:  # extract number of spills
                text_numnber = extract_text_from_page(pdf, page_number)
                df_nspills = extract_spill_number(text_numnber)
                assert df_nspills.index.is_unique
                df_nspills.reset_index(inplace = True)

            elif page_number == 4: # extract biggest spills in history (not currently used; just in case we want it at some point)
                textmj = extract_text_from_page(pdf, page_number)
                df_biggest_spills = extract_biggest_spills(textmj)
                df_biggest_spills.reset_index(inplace = True)

            elif page_number == 15: # extract cause and operations data
                text_cause = extract_text_from_page(pdf, page_number)
                df_above_7000, df_7_7000 = extract_cause_data(text_cause)


    nsp_quant = pd.merge(df_nspills, df_oil_spilled, on = 'year', how = 'outer')  # Extract and merge oil spilled and number of spills
    nsp_quant['country'] = 'World'  # add World

    # Causes
    df_above_7000_cause_totals = df_above_7000[['Cause', df_above_7000.columns[-1]]].copy()
    df_below_7000_cause_totals = df_7_7000[['Cause', df_7_7000.columns[-1]]].copy()

    df_below_7000_cause_totals['year'] = 2023
    df_above_7000_cause_totals['year'] = 2023
    df_below_7000_cause_totals_pv = df_below_7000_cause_totals.pivot(index='year', columns='Cause', values='Total')
    df_above_7000_cause_totals_pv = df_above_7000_cause_totals.pivot(index='year', columns='Cause', values='Total')
    df_below_7000_cause_totals_pv = df_below_7000_cause_totals_pv.rename_axis(None, axis='columns')
    df_below_7000_cause_totals_pv.reset_index(inplace=True)

    df_above_7000_cause_totals_pv = df_above_7000_cause_totals_pv.rename_axis(None, axis='columns')
    df_above_7000_cause_totals_pv.reset_index(inplace=True)
    df_below_7000_cause_totals_pv['country'] = 'Small (7-700t)'
    df_above_7000_cause_totals_pv['country'] = 'Large (>700t)'
    merged_causes = pd.concat([df_above_7000_cause_totals_pv, df_below_7000_cause_totals_pv], axis=0)
    for column in merged_causes.columns:
        if column not in ['year', 'country']:
            merged_causes.rename(columns={column: column + '_causes'}, inplace=True)


    # Operations

    df_above_7000[df_above_7000.columns[1:]] = df_above_7000[df_above_7000.columns[1:]].astype(int)
    df_above_7000.loc['Operations Total'] = df_above_7000.sum(axis=0)
    operations_ab_7000 = df_above_7000.iloc[[-1]]

    df_7_7000[df_7_7000.columns[1:]] = df_7_7000[df_7_7000.columns[1:]].astype(int)
    df_7_7000.loc['Operations Total'] = df_7_7000.sum(axis=0)
    operations_bel_7000 = df_7_7000.iloc[[-1]]

    operations_total = pd.concat([operations_bel_7000, operations_ab_7000])

    operations_total.index = ['Small (7-700t)', 'Large (>700t)']
    operations_total = operations_total.drop('Cause', axis=1)
    operations_total.reset_index(inplace = True)

    operations_total.rename(columns = {'index': 'country'},inplace=True)
    operations_total['year'] = 2023
    for column in operations_total.columns:
        if column not in ['year', 'country']:
            operations_total.rename(columns={column: column + '_ops'}, inplace=True)
    merge_cause_op = pd.merge(merged_causes, operations_total, on = ['year', 'country'])
    combined_df = pd.merge(nsp_quant,merge_cause_op, on = ['year', 'country'], how = 'outer')
    combined_df.drop('Total_ops', axis = 1, inplace = True)
    merge_biggest_spills = pd.merge(combined_df, df_biggest_spills, on = ['year', 'country'], how = 'outer')


    # Create a new table and ensure all columns are snake-case.
    tb = Table(merge_biggest_spills, short_name=paths.short_name, underscore=True)

    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("oil_spills.end")




def extract_pdf_content(url):
    """
    Extracts the content of a PDF file from a given URL.

    Args:
        url (str): URL of the PDF file.

    Returns:
        bytes: The content of the PDF file.
    """
    with urllib.request.urlopen(url) as response:
        pdf_content = response.read()
    return pdf_content


def extract_text_from_page(pdf, page_number):
    """
    Extracts the text from a specific page of a PDF.

    Args:
        pdf (pdfplumber.PDF): The PDF object.
        page_number (int): The page number to extract text from.

    Returns:
        str: The extracted text.
    """
    page = pdf.pages[page_number]
    return page.extract_text()

def extract_spill_quantity(text):
    """
    Extracts oil spill quantity data from the given text (assumes the 2023 version of the table).

    Args:
        text (str): The text containing oil spill data.

    Returns:
        pd.DataFrame: DataFrame with extracted oil spill data.
    """

    # Define pattern to extract oil spill quantity
    pattern = r"(?<!\d)(?:19[7-9]\d|20[0-2]\d)\s+(\d{1,3}(?:,\d{3})*)"

    # Extract matches of oil spill quantity
    matches = re.findall(pattern, text)

    # Define pattern to extract year
    pattern_year = r"\b(19[7-9]\d|20[0-2]\d)\b(?=(?:\s+\d{1,3}(?:,\d{3})*)\b)"

    # Extract matches of year
    matches_year = re.findall(pattern_year, text)

    # Convert year matches to integer
    years = [int(year) for year in matches_year]

    # Convert oil spill quantity matches to integer
    oil_spilled = [int(oil.replace(',', '')) for oil in matches]

    # Create DataFrame with extracted oil spill data
    df_oil_spills = pd.DataFrame({'year': years, 'oil_spilled': oil_spilled})
    df_oil_spills.sort_values('year', inplace=True)
    df_oil_spills.set_index('year', inplace=True)

    return df_oil_spills

def extract_spill_number(text):
    """
    Extracts number of oil spills data from the given text (assumes the 2023 version of the table).

    Args:
        text (str): The text containing spill data.

    Returns:
        pd.DataFrame: DataFrame with extracted spill data.
    """

    # Define pattern to extract number of spills data
    pattern = r"(?<!\d)(19[7-9]\d|20[0-1]\d|2020|2021|2022)(?:\s+(\d+)\s+(\d+))?"

    # Extract matches of number of spills data
    matches = re.findall(pattern, text)

    data = []
    for match in matches:
        year = match[0]
        value1 = match[1]
        value2 = match[2]

        if value1 != '' and value2 != '':
            data.append([year, value1, value2])

    # Create DataFrame with extracted number of spills data
    df_spills = pd.DataFrame(data, columns=['year', 'bel_700t', 'ab_700t'])
    df_spills.sort_values('year', inplace=True)

    # Convert columns to integer type
    for column in df_spills.columns:
        df_spills[column] = df_spills[column].astype(int)

    df_spills.set_index('year', inplace = True)

    return df_spills

def extract_biggest_spills(text):
    """
    Extracts the biggest spills data from the given text (assumes the 2023 version of the table).

    Args:
        text (str): The text containing biggest spills data.

    Returns:
        pd.DataFrame: Dataframe with extracted biggest spills data.
    """
    pattern = r"\n(\d+)\s(.+?)\s+(\d{4})\s(.+?)\s(\d{1,3}(?:,\d{3})*)"
    matches = re.findall(pattern, text)

    years = []; locations = []; spill_sizes = []

    for match in matches:
        year = int(match[2])
        location = match[3]
        spill_size = match[4].replace(",", "")

        years.append(year)
        locations.append(location)
        spill_sizes.append(spill_size)

    df_biggest_spills = pd.DataFrame({'year': years, 'country': locations, 'biggest_spills_size': spill_sizes})
    df_biggest_spills['biggest_spills_size'] = df_biggest_spills['biggest_spills_size'].astype(int)

    df_biggest_spills.set_index('year', inplace=True)

    return df_biggest_spills


def extract_cause_data(text):
    """
    Extracts the cause data from the given text (assumes the 2023 version of the table).

    Args:
        text (str): The text containing cause data.

    Returns:
        pd.DataFrame: Dataframe with extracted cause data.
    """
    names = ["Allision/Collision", "Grounding", "Hull Failure", "Equipment Failure", "Fire/Explosion", "Other", "Unknown"]

    split_text = re.split(r"Table 4:[\s\S]+?\n[A-Za-z0-9 ]", text)
    before_table4 = split_text[0]
    after_table4 = split_text[1]

    name_number_pairs_4 = []; name_number_pairs_5 = []

    for name in names:
        pattern = r"({})\s+(\d+\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+)".format(name)
        match = re.search(pattern, before_table4)
        if match:
            numbers = match.group(2).split()
            name_number_pairs_4.append((name, *numbers))

    for name in names:
        pattern = r"({})\s+(\d+\s+\d+\s+\d+\s+\d+\s+\d+)".format(name)
        match = re.search(pattern, after_table4)
        if match:
            numbers = match.group(2).split()
            name_number_pairs_5.append((name, *numbers))

    df_above_7000 = pd.DataFrame(name_number_pairs_4, columns=["Cause", "At Anchor (inland)", "At Anchor (open Water)", "Underway (inland)", "Underway (open water)", "Loading/Discharing", "Other Operations", "Total"])
    df_7_7000 = pd.DataFrame(name_number_pairs_5, columns=["Cause", "Loading/Discharing", "Bunkering", "Other Operations", "Uknown", "Total"])

    return df_above_7000, df_7_7000

