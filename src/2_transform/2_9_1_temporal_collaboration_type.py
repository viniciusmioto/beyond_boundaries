import pandas as pd
import json

# Define input and output paths based on your requirements
INPUT_PATH = "../../data/processed/6_publications_collab_types.csv"
OUTPUT_PATH = "../../data/processed/7_temporal_collab_types.csv"


def process_base_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepares the raw DataFrame by parsing necessary fields.
    Adds 'subfield_display' and 'is_single_subfield' columns.
    """

    def get_subfield_display(subfield_json: str):
        """Extracts subfield display name from JSON string."""
        try:
            # Ensure subfield_json is a string; handle potential float NaNs if they were not strings
            if pd.isna(subfield_json) or not isinstance(subfield_json, str):
                return None
            return json.loads(subfield_json).get("display_name")
        except (json.JSONDecodeError, TypeError):
            # Return None if JSON is invalid or input is not string-like
            return None

    processed_df = df.copy()

    # Extract subfield display name
    # Apply astype(str) to handle potential non-string types before passing to json.loads
    processed_df["subfield_display"] = processed_df["subfield"].astype(str).apply(
        get_subfield_display
    )

    # Determine if a publication is single-subfield
    # Assumes 'collab_type' column contains "Single-Subfield" or "Multi-Subfield"
    # NaNs or other values in 'collab_type' would result in is_single_subfield being False
    processed_df["is_single_subfield"] = processed_df["collab_type"].astype(str) == "Single-Subfield"
    
    # Drop rows where subfield_display is None, as they cannot be grouped by subfield
    processed_df.dropna(subset=['subfield_display'], inplace=True)

    return processed_df


def summarize_subfield_publications(processed_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generates summary statistics for publications, grouped by subfield and year.
    Calculates counts, percentages, and average citations for single-subfield
    and multi-subfield publications.
    """
    # Ensure 'cited_by_count' is numeric, coercing errors to NaN (which will then be handled)
    processed_df['cited_by_count'] = pd.to_numeric(processed_df['cited_by_count'], errors='coerce')

    # Calculate average citations per subfield, year, and collaboration type
    # is_single_subfield = True means Single-Subfield
    # is_single_subfield = False means Multi-Subfield
    citation_avg = (
        processed_df.groupby(["subfield_display", "publication_year", "is_single_subfield"])["cited_by_count"]
        .mean()
        .unstack()  # Unstack 'is_single_subfield' level to columns (True, False)
    )
    # Rename columns: True -> single_subfield_avg_citations, False -> multi_subfield_avg_citations
    citation_avg.rename(
        columns={True: "single_subfield_avg_citations", False: "multi_subfield_avg_citations"},
        inplace=True,
    )

    # Calculate publication counts per subfield and year
    # 'is_single_subfield' is boolean, sum() will count True values (single-subfield publications)
    summary = (
        processed_df.groupby(["subfield_display", "publication_year"])
        .agg(
            single_subfield_publications=("is_single_subfield", "sum"),  # Counts where is_single_subfield is True
            total_publications=("is_single_subfield", "count"),  # Total count of rows for the group
        )
        .reset_index()
    )

    # Merge citation averages with publication counts
    summary = summary.merge(citation_avg, on=["subfield_display", "publication_year"], how="left")

    # Calculate derived metrics
    summary["multi_subfield_publications"] = summary["total_publications"] - summary["single_subfield_publications"]
    
    # Calculate percentages, handling division by zero by resulting in NaN (which is fine)
    summary["single_subfield_percentage"] = (
        summary["single_subfield_publications"] / summary["total_publications"] * 100
    )
    summary["multi_subfield_percentage"] = (
        summary["multi_subfield_publications"] / summary["total_publications"] * 100
    )
    
    # Round percentages after calculation
    summary["single_subfield_percentage"] = summary["single_subfield_percentage"].round(2)
    summary["multi_subfield_percentage"] = summary["multi_subfield_percentage"].round(2)


    # Fill NaN in citation columns with 0 (e.g., if a category has no publications or no citations)
    # This happens if a subfield/year only has single OR multi-subfield pubs, not both.
    if "single_subfield_avg_citations" not in summary.columns:
        summary["single_subfield_avg_citations"] = 0.0
    else:
        summary["single_subfield_avg_citations"] = summary["single_subfield_avg_citations"].fillna(0)
    
    if "multi_subfield_avg_citations" not in summary.columns:
        summary["multi_subfield_avg_citations"] = 0.0
    else:
        summary["multi_subfield_avg_citations"] = summary["multi_subfield_avg_citations"].fillna(0)


    return summary[
        [
            "subfield_display",
            "publication_year",
            "single_subfield_publications",
            "multi_subfield_publications",
            "total_publications",
            "single_subfield_percentage",
            "multi_subfield_percentage",
            "single_subfield_avg_citations",
            "multi_subfield_avg_citations",
        ]
    ]


def add_yearly_totals(summary_df: pd.DataFrame, processed_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates and appends cross-subfield totals for each year to the summary DataFrame.
    """
    # Ensure 'cited_by_count' is numeric in processed_df for yearly totals calculation
    processed_df['cited_by_count'] = pd.to_numeric(processed_df['cited_by_count'], errors='coerce')

    # Aggregate yearly totals from processed data
    yearly_totals = processed_df.groupby("publication_year").apply(
        lambda g: pd.Series({
            "single_subfield_publications": g["is_single_subfield"].sum(),
            "total_publications": len(g),
            # Average citations for single-subfield publications
            "single_subfield_avg_citations": g[g["is_single_subfield"]]["cited_by_count"].mean(),
            # Average citations for multi-subfield publications (where is_single_subfield is False)
            "multi_subfield_avg_citations": g[~g["is_single_subfield"]]["cited_by_count"].mean()
        })
    ).reset_index()

    # Calculate derived fields for yearly totals
    yearly_totals["multi_subfield_publications"] = yearly_totals["total_publications"] - yearly_totals["single_subfield_publications"]
    
    yearly_totals["single_subfield_percentage"] = (
        yearly_totals["single_subfield_publications"] / yearly_totals["total_publications"] * 100
    )
    yearly_totals["multi_subfield_percentage"] = (
        yearly_totals["multi_subfield_publications"] / yearly_totals["total_publications"] * 100
    )

    yearly_totals["single_subfield_percentage"] = yearly_totals["single_subfield_percentage"].round(2)
    yearly_totals["multi_subfield_percentage"] = yearly_totals["multi_subfield_percentage"].round(2)
    
    yearly_totals["subfield_display"] = "Total"

    # Fill NaN in citation columns (e.g., if a year has only single or only multi-subfield pubs)
    yearly_totals["single_subfield_avg_citations"] = yearly_totals["single_subfield_avg_citations"].fillna(0)
    yearly_totals["multi_subfield_avg_citations"] = yearly_totals["multi_subfield_avg_citations"].fillna(0)

    # Reorder columns to match summary_df structure
    # Ensure summary_df.columns has the correct list of new column names
    yearly_totals = yearly_totals[summary_df.columns]

    # Combine with original summary
    return pd.concat([summary_df, yearly_totals], ignore_index=True)


# Main execution flow
if __name__ == "__main__":
    print(f"Reading publications from {INPUT_PATH}...")
    try:
        publications_df = pd.read_csv(INPUT_PATH)
        print("Successfully read publications.")
    except FileNotFoundError:
        print(f"Error: Input file not found at {INPUT_PATH}")
        exit()

    print("Processing base data...")
    processed_publications_df = process_base_data(publications_df)
    print(f"Processed base data. Shape: {processed_publications_df.shape}")
    if processed_publications_df.empty:
        print("No data available after initial processing (e.g., all subfields were invalid). Exiting.")
        exit()

    print("Creating temporal subfield summary...")
    subfield_summary_df = summarize_subfield_publications(processed_publications_df)
    print(f"Created temporal subfield summary. Shape: {subfield_summary_df.shape}")

    if subfield_summary_df.empty:
         print("Subfield summary is empty (e.g. no valid subfield/year groups). Skipping yearly totals and saving empty summary.")
    else:
        print("Adding yearly totals...")
        final_summary_df = add_yearly_totals(subfield_summary_df, processed_publications_df)
        print(f"Added yearly totals. Shape: {final_summary_df.shape}")

        # Sort by year and then by subfield_display (with "Total" appearing last within each year)
        final_summary_df["sort_key"] = final_summary_df["subfield_display"].apply(lambda x: (x == "Total") * 1)
        final_summary_df.sort_values(
            by=["publication_year", "sort_key", "subfield_display"],
            ascending=[True, True, True],
            inplace=True
        )
        final_summary_df.drop("sort_key", axis=1, inplace=True)
    
    # If subfield_summary_df was empty, final_summary_df might not be defined.
    # In that case, we save the empty subfield_summary_df.
    output_df = final_summary_df if not subfield_summary_df.empty else subfield_summary_df

    output_df.to_csv(OUTPUT_PATH, index=False)
    print(f"Complete temporal summary of collaboration types saved to {OUTPUT_PATH}")