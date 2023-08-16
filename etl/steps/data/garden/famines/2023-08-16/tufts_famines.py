"""Load a meadow dataset and create a garden dataset."""

from typing import cast

from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("tufts_famines"))

    # Read table from meadow dataset.
    tb = ds_meadow["tufts_famines"].reset_index()

    #
    # Process data.
    #
    tb: Table = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def clean_up_causes(df):
    df["cause"] = df["cause"].replace(
        "Drought, lack of state capacity due to rebellion & colonialism",
        "Drought, lack of state capacity due to rebellion, colonialism",
    )
    df["cause"] = df["cause"].replace(
        "Drought, economic crisis, colonial warfare, internal rebellion",
        "Drought, economic crisis, colonialism, war, internal rebellion",
    )
    df["cause"] = df["cause"].replace("Govt wartime policy", "Government wartime policy")
    df["cause"] = df["cause"].replace("Hunger Plan", "War, Genocide")
    df["cause"] = df["cause"].replace("Sanctions, war and dictatorship", "Sanctions, war, dictatorship")

    # Refined categories
    refined_categories = {
        "economic crisis": "Economic challenges",
        "drought": "Environmental events",
        "lack of state capacity due to rebellion": "Governance and policy",
        "colonialism": "External interventions",
        "forced labor": "Governance and policy",
        "war": "War and armed conflicts",
        "rinderpest": "Environmental events",
        "internal rebellion": "War and armed eonflicts",
        "boer war camps": "External interventions",
        "genocide": "Repression and Persecution",
        "repression of rebellion": "Governance and Policy",
        "colonial conquest": "External Interventions",
        "blockade": "External Interventions",
        "locusts": "Environmental Events",
        "forced deportation": "Repression and Persecution",
        "post-conflict": "War and Armed Conflicts",
        "civil war": "War and Armed Conflicts",
        "war between chiang kai-shek and warlords": "War and Armed Conflicts",
        "concentration camps": "Repression and Persecution",
        "collectivization": "Governance and Policy",
        "death of german pows in soviet captivity": "War and Armed Conflicts",
        "japanese soldiers who died of malnutrition and starvation": "War and Armed Conflicts",
        "japanese occupation": "War and Armed Conflicts",
        "government policy": "Governance and Policy",
        "reprisals against germans": "War and Armed Conflicts",
        "food shortage and policy": "Governance and Policy",
        "govt policies": "Governance and Policy",
        "war/blockade": "War and Armed Conflicts",
        "flood": "Environmental Events",
        "cyclones": "Environmental Events",
        "conflict": "War and Armed Conflicts",
        "year zero": "Governance and Policy",
        "sanctions": "External Interventions",
        "dictatorship": "Governance and Policy",
        "food shortage and govt policy": "Governance and Policy",
    }

    # Function to categorize causes into broad categories
    def categorize_cause(cause):
        individual_causes = cause.lower().split(", ")
        categories_for_cause = set()
        for ic in individual_causes:
            if ic in refined_categories:
                categories_for_cause.add(refined_categories[ic])
        return ", ".join(list(categories_for_cause)[:2])

    # Apply function to each cause in the list
    df["broad_categories"] = df["cause"].apply(categorize_cause)

    return df
