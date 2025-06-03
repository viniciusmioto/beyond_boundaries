import pandas as pd
import json

PUBLICATIONS_PATH = "../../data/raw/publication_meta/br_publication_meta.csv"
OUTPUT_PATH = "../../data/processed/4_2_international_proportion_temporal.csv"


def process_base_data(df: pd.DataFrame) -> pd.DataFrame:
    """Add required columns to the raw data."""

    def get_subfield_display(subfield_json: str):
        try:
            return json.loads(subfield_json).get("display_name")
        except (json.JSONDecodeError, TypeError):
            return None

    def is_domestic_publication(authorships_json: str) -> bool:
        try:
            authors = json.loads(authorships_json)
        except (json.JSONDecodeError, TypeError):
            return False
        for author in authors:
            countries = author.get("countries", [])
            if any(country != "BR" for country in countries):
                return False
        return True

    processed_df = df.copy()
    processed_df["subfield_display"] = processed_df["subfield"].apply(
        get_subfield_display
    )
    processed_df["is_domestic"] = processed_df["authorships"].apply(
        is_domestic_publication
    )
    return processed_df


def summarize_subfield_publications(processed_df: pd.DataFrame) -> pd.DataFrame:
    """Generate summary statistics from processed data, grouped by subfield and year."""
    # Calculate average citations per subfield, year, and collaboration type
    citation_avg = (
        processed_df.groupby(["subfield_display", "publication_year", "is_domestic"])["cited_by_count"]
        .mean()
        .unstack()
    )
    citation_avg.rename(
        columns={True: "domestic_avg_citations", False: "international_avg_citations"},
        inplace=True,
    )

    # Calculate publication counts per subfield and year
    summary = (
        processed_df.groupby(["subfield_display", "publication_year"])
        .agg(
            domestic_publications=("is_domestic", "sum"),
            total_publications=("is_domestic", "count"),
        )
        .reset_index()
    )

    # Merge citation averages with publication counts
    summary = summary.merge(citation_avg, on=["subfield_display", "publication_year"], how="left")

    # Calculate derived metrics
    summary["international_publications"] = summary["total_publications"] - summary["domestic_publications"]
    summary["domestic_percentage"] = (
        summary["domestic_publications"] / summary["total_publications"] * 100
    ).round(2)
    summary["international_percentage"] = (
        summary["international_publications"] / summary["total_publications"] * 100
    ).round(2)

    return summary[
        [
            "subfield_display",
            "publication_year",
            "domestic_publications",
            "international_publications",
            "total_publications",
            "domestic_percentage",
            "international_percentage",
            "domestic_avg_citations",
            "international_avg_citations",
        ]
    ]


def add_yearly_totals(summary: pd.DataFrame, processed_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate cross-subfield totals for each year."""
    # Aggregate yearly totals from processed data
    yearly_totals = processed_df.groupby("publication_year").apply(
        lambda g: pd.Series({
            "domestic_publications": g["is_domestic"].sum(),
            "total_publications": len(g),
            "domestic_avg_citations": g[g["is_domestic"]]["cited_by_count"].mean(),
            "international_avg_citations": g[~g["is_domestic"]]["cited_by_count"].mean()
        })
    ).reset_index()

    # Calculate derived fields
    yearly_totals["international_publications"] = yearly_totals["total_publications"] - yearly_totals["domestic_publications"]
    yearly_totals["domestic_percentage"] = (yearly_totals["domestic_publications"] / yearly_totals["total_publications"] * 100).round(2)
    yearly_totals["international_percentage"] = (yearly_totals["international_publications"] / yearly_totals["total_publications"] * 100).round(2)
    yearly_totals["subfield_display"] = "Total"

    # Reorder columns to match summary structure
    yearly_totals = yearly_totals[summary.columns]

    # Combine with original summary
    return pd.concat([summary, yearly_totals], ignore_index=True)


# Main execution flow
publications_df = pd.read_csv(PUBLICATIONS_PATH)
print("Read publications")

processed_df = process_base_data(publications_df)
print("Processed base data")

summary_df = summarize_subfield_publications(processed_df)
print("Created temporal subfield summary")

final_summary = add_yearly_totals(summary_df, processed_df)

# Sort by year and subfield (Total last within each year)
final_summary["sort_key"] = final_summary["subfield_display"].apply(lambda x: (x == "Total") * 1)
final_summary.sort_values(
    by=["publication_year", "sort_key", "subfield_display"],
    ascending=[True, True, True],
    inplace=True
)
final_summary.drop("sort_key", axis=1, inplace=True)

final_summary.to_csv(OUTPUT_PATH, index=False)
print(f"Complete temporal summary saved to {OUTPUT_PATH}")