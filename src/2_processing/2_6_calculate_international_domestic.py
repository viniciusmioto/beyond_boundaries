import pandas as pd
import json

PUBLICATIONS_PATH = "../../data/raw/publication_meta/br_publication_meta.csv"
OUTPUT_PATH = "../../data/processed/4_international_proportion.csv"


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
    """Generate summary statistics from processed data."""
    citation_avg = (
        processed_df.groupby(["subfield_display", "is_domestic"])["cited_by_count"]
        .mean()
        .unstack()
    )
    citation_avg.rename(
        columns={True: "domestic_avg_citations", False: "international_avg_citations"},
        inplace=True,
    )

    summary = (
        processed_df.groupby("subfield_display")
        .agg(
            domestic_publications=("is_domestic", "sum"),
            total_publications=("is_domestic", "count"),
        )
        .reset_index()
    )

    summary = summary.merge(citation_avg, on="subfield_display", how="left")
    summary["international_publications"] = (
        summary["total_publications"] - summary["domestic_publications"]
    )
    summary["domestic_percentage"] = (
        summary["domestic_publications"] / summary["total_publications"] * 100
    ).round(2)
    summary["international_percentage"] = (
        summary["international_publications"] / summary["total_publications"] * 100
    ).round(2)

    summary["domestic_avg_citations"] = (
        summary["domestic_avg_citations"] / summary["total_publications"] * 100
    ).round(2)
    summary["international_avg_citations"] = (
        summary["international_avg_citations"] / summary["total_publications"] * 100
    ).round(2)

    return summary[
        [
            "subfield_display",
            "domestic_publications",
            "international_publications",
            "total_publications",
            "domestic_percentage",
            "international_percentage",
            "domestic_avg_citations",
            "international_avg_citations",
        ]
    ]


def add_total_row(summary: pd.DataFrame, processed_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate cross-subfield totals using processed data."""
    total_domestic = processed_df["is_domestic"].sum()
    total_publications = len(processed_df)
    total_international = total_publications - total_domestic

    total_row = {
        "subfield_display": "Total",
        "domestic_publications": total_domestic,
        "international_publications": total_international,
        "total_publications": total_publications,
        "domestic_percentage": round((total_domestic / total_publications) * 100, 2),
        "international_percentage": round(
            (total_international / total_publications) * 100, 2
        ),
        "domestic_avg_citations": processed_df[processed_df["is_domestic"]][
            "cited_by_count"
        ].mean(),
        "international_avg_citations": processed_df[~processed_df["is_domestic"]][
            "cited_by_count"
        ].mean(),
    }

    return pd.concat([summary, pd.DataFrame([total_row])], ignore_index=True)


# Main execution flow
publications_df = pd.read_csv(PUBLICATIONS_PATH)
print("Read publications")

processed_df = process_base_data(publications_df)
print("Processed base data")

summary_df = summarize_subfield_publications(processed_df)
print("Created subfield summary")

summary_df.sort_values(by="domestic_percentage", inplace=True)
final_summary = add_total_row(summary_df, processed_df)  # Use processed_df here
final_summary.to_csv(OUTPUT_PATH, index=False)
print(f"Complete summary saved to {OUTPUT_PATH}")
