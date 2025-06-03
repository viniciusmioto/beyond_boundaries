import pandas as pd
import json
import ast

INPUT_PATH = "../../data/processed/6_publications_collab_types.csv"
AUTHORS_PATH = "../../data/processed/3_authors.csv"
OUTPUT_PATH = "../../data/processed/8_subfield_comparison.csv"


def is_single_collaboration(author_ids: list, author_subfield_map: dict) -> bool:
    """Determine if a publication represents single-subfield collaboration."""
    subfields = []
    for author_id in author_ids:
        subfield = author_subfield_map.get(author_id)
        if subfield and subfield != 'unknown' and not pd.isna(subfield):
            subfields.append(subfield)
    return len(set(subfields)) <= 1  # True if 0 or 1 unique subfields


def process_base_data(df: pd.DataFrame, author_subfield_map: dict) -> pd.DataFrame:
    """Enhance raw data with collaboration analysis."""
    
    def get_subfield_display(subfield_json: str):
        try:
            return json.loads(subfield_json).get("display_name")
        except (json.JSONDecodeError, TypeError):
            return None

    processed_df = df.copy()
    
    # Extract publication subfield
    processed_df["subfield_display"] = processed_df["subfield"].apply(get_subfield_display)
    
    # Extract author IDs from authorships
    processed_df["author_ids"] = processed_df["authorships"].apply(
        lambda x: [a["id"] for a in ast.literal_eval(x)] if isinstance(x, str) else []
    )
    
    # Calculate collaboration type
    processed_df["is_single"] = processed_df["author_ids"].apply(
        lambda ids: is_single_collaboration(ids, author_subfield_map)
    )
    
    return processed_df


def summarize_subfield_publications(processed_df: pd.DataFrame) -> pd.DataFrame:
    """Generate subfield-level statistics with clean data handling."""
    # Calculate citation averages
    citation_avg = (
        processed_df.groupby(["subfield_display", "is_single"])["cited_by_count"]
        .mean()
        .unstack()
    )
    citation_avg.rename(
        columns={True: "single_avg_citations", False: "multi_avg_citations"},
        inplace=True,
    )

    # Aggregate publication counts
    summary = (
        processed_df.groupby("subfield_display")
        .agg(
            single_publications=("is_single", "sum"),
            total_publications=("is_single", "count"),
        )
        .reset_index()
    )

    # Merge metrics and calculate percentages
    summary = summary.merge(citation_avg, on="subfield_display", how="left")
    summary["multi_publications"] = summary["total_publications"] - summary["single_publications"]
    
    for col in ["single", "multi"]:
        summary[f"{col}_percentage"] = (
            summary[f"{col}_publications"] / summary["total_publications"] * 100
        ).round(2)
    
    return summary[
        [
            "subfield_display",
            "single_publications",
            "multi_publications",
            "total_publications",
            "single_percentage",
            "multi_percentage",
            "single_avg_citations",
            "multi_avg_citations",
        ]
    ]


def add_total_row(summary: pd.DataFrame, processed_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate cross-subfield totals with proper aggregation."""
    total_single = processed_df["is_single"].sum()
    total_publications = len(processed_df)
    total_multi = total_publications - total_single

    total_row = {
        "subfield_display": "Total",
        "single_publications": total_single,
        "multi_publications": total_multi,
        "total_publications": total_publications,
        "single_percentage": round((total_single / total_publications) * 100, 2),
        "multi_percentage": round((total_multi / total_publications) * 100, 2),
        "single_avg_citations": processed_df[processed_df["is_single"]][
            "cited_by_count"
        ].mean(),
        "multi_avg_citations": processed_df[~processed_df["is_single"]][
            "cited_by_count"
        ].mean(),
    }

    return pd.concat([summary, pd.DataFrame([total_row])], ignore_index=True)


# Main execution
if __name__ == "__main__":
    # Load data
    publications_df = pd.read_csv(INPUT_PATH)
    authors_df = pd.read_csv(AUTHORS_PATH)
    author_subfield_map = authors_df.set_index('id')['primary_subfield'].to_dict()
    
    print("Loaded data sources")
    
    # Process data
    processed_df = process_base_data(publications_df, author_subfield_map)
    summary_df = summarize_subfield_publications(processed_df)
    
    # Finalize and save
    summary_df.sort_values(by="single_percentage", inplace=True)
    final_summary = add_total_row(summary_df, processed_df)
    final_summary.to_csv(OUTPUT_PATH, index=False)
    
    print(f"Analysis complete. Results saved to {OUTPUT_PATH}")