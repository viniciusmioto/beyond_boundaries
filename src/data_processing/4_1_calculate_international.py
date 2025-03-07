import pandas as pd
import json


PUBLICATIONS_PATH = "../../data/raw/publication_meta/br_publication_meta.csv"
OUTPUT_PATH = "../../data/processed/4_international_proportion.csv"


def summarize_subfield_publications(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each subfield, computes the number and percentage of publications that are:
      - Domestic only: all authors have affiliations exclusively to Brazilian institutions.
      - International collaborations: at least one author is affiliated with a non-Brazilian institution.

    The function returns a DataFrame with the following columns:
      - subfield_display: the subfield's display name.
      - domestic_publications: count of publications with only Brazilian affiliations.
      - international_publications: count of publications with any international affiliation.
      - total_publications: total count of publications for that subfield.
      - domestic_percentage: percentage of domestic publications.
      - international_percentage: percentage of international collaboration publications.

    Parameters:
        df (pd.DataFrame): The input DataFrame containing at least 'authorships' and 'subfield' columns.

    Returns:
        pd.DataFrame: Summary table as described above.
    """

    # Helper function to parse the subfield display name
    def get_subfield_display(subfield_json: str):
        try:
            return json.loads(subfield_json).get("display_name")
        except (json.JSONDecodeError, TypeError):
            return None

    # Helper function to check if publication is domestic-only.
    # It returns True if every author has affiliations only to Brazil ("BR").
    def is_domestic_publication(authorships_json: str) -> bool:
        try:
            authors = json.loads(authorships_json)
        except (json.JSONDecodeError, TypeError):
            # if parsing fails, treat publication as not domestic
            return False
        for author in authors:
            countries = author.get("countries", [])
            # if any country is not "BR", then publication is international
            if any(country != "BR" for country in countries):
                return False
        return True

    # Create new columns in a copy of the DataFrame
    df = df.copy()
    df["subfield_display"] = df["subfield"].apply(get_subfield_display)
    df["is_domestic"] = df["authorships"].apply(is_domestic_publication)

    # Group by subfield and compute counts
    summary = (
        df.groupby("subfield_display")
        .agg(
            domestic_publications=("is_domestic", "sum"),
            total_publications=("is_domestic", "count"),
        )
        .reset_index()
    )

    # Calculate international counts and percentages
    summary["international_publications"] = (
        summary["total_publications"] - summary["domestic_publications"]
    )
    summary["domestic_percentage"] = (
        summary["domestic_publications"] / summary["total_publications"] * 100
    )
    summary["international_percentage"] = (
        summary["international_publications"] / summary["total_publications"] * 100
    )

    # Optional: Order columns for readability
    summary = summary[
        [
            "subfield_display",
            "domestic_publications",
            "international_publications",
            "total_publications",
            "domestic_percentage",
            "international_percentage",
        ]
    ]

    return summary



publications_df = pd.read_csv(PUBLICATIONS_PATH)
print("Read publications")
summary_df = summarize_subfield_publications(publications_df)
print("Created summary")
summary_df.sort_values(by=["domestic_percentage", "international_percentage",], inplace=True)
summary_df.to_csv(OUTPUT_PATH)
print(f"Summary saved to {OUTPUT_PATH}")
