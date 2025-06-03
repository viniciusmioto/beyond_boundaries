import pandas as pd
import json
import numpy as np

# Define paths for input and output files
PUBLICATIONS_PATH = "../../data/raw/publication_meta/br_publication_meta.csv"
# Output for table 1: Publication counts and proportions
OUTPUT_PATH_PROPORTIONS = "../../data/processed/4_0_international_proportion.csv"
# Output for table 2: Detailed citation metrics by subfield and collaboration type
OUTPUT_PATH_DETAILED_METRICS = "../../data/processed/4_1_international_domestic_detailed_metrics.csv"

def process_base_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds necessary columns ('subfield_display', 'is_domestic') to the raw DataFrame.
    Handles potential errors in JSON parsing for 'subfield' and 'authorships'.
    """
    processed_df = df.copy()

    def get_subfield_display(subfield_json: str):
        try:
            data = json.loads(subfield_json)
            return data.get("display_name")
        except (json.JSONDecodeError, TypeError, AttributeError):
            return None # Return None if JSON is invalid or not a dictionary

    def is_domestic_publication(authorships_json: str) -> bool:
        try:
            authors = json.loads(authorships_json)
            if not isinstance(authors, list): # Ensure authors is a list
                return True # Or False, depending on how to handle malformed - let's assume domestic if unclear/problematic
            
            for author in authors:
                if not isinstance(author, dict): # Ensure author is a dictionary
                    continue # Skip malformed author entries
                countries = author.get("countries", [])
                if not isinstance(countries, list): # Ensure countries is a list
                    continue # Skip malformed countries entries
                if any(country != "BR" for country in countries if isinstance(country, str)):
                    return False # Found an international author
            return True # All authors are domestic or have no country info treated as domestic
        except (json.JSONDecodeError, TypeError):
            return True # Default to domestic if authorships is unparsable, or handle as error

    processed_df["subfield_display"] = processed_df["subfield"].apply(get_subfield_display)
    processed_df["authorships"] = processed_df["authorships"].astype(str) # Ensure it's string for JSON processing
    processed_df["is_domestic"] = processed_df["authorships"].apply(is_domestic_publication)
    
    # Drop rows where subfield_display could not be parsed, as they can't be grouped
    processed_df.dropna(subset=['subfield_display'], inplace=True)
    
    return processed_df

def summarize_publication_proportions(processed_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generates summary of publication counts and percentages for domestic vs. international.
    This data is for the first output table.
    """
    summary = (
        processed_df.groupby("subfield_display")
        .agg(
            domestic_publications=("is_domestic", "sum"),
            total_publications=("is_domestic", "count"),
        )
        .reset_index()
    )

    summary["international_publications"] = (
        summary["total_publications"] - summary["domestic_publications"]
    )
    summary["domestic_percentage"] = (
        (summary["domestic_publications"] / summary["total_publications"]) * 100
    )
    summary["international_percentage"] = (
        (summary["international_publications"] / summary["total_publications"]) * 100
    )
    
    # Round percentages
    summary["domestic_percentage"] = summary["domestic_percentage"].round(2)
    summary["international_percentage"] = summary["international_percentage"].round(2)

    return summary[
        [
            "subfield_display",
            "domestic_publications",
            "international_publications",
            "total_publications",
            "domestic_percentage",
            "international_percentage",
        ]
    ]

def add_total_row_proportions(summary_df: pd.DataFrame, processed_df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a 'Total' row to the proportions summary DataFrame.
    """
    if processed_df.empty:
        print("Warning: Processed DataFrame is empty. Cannot calculate totals for proportions.")
        return summary_df

    total_domestic = processed_df["is_domestic"].sum()
    total_publications = len(processed_df)
    total_international = total_publications - total_domestic

    total_row_dict = {
        "subfield_display": "Total",
        "domestic_publications": total_domestic,
        "international_publications": total_international,
        "total_publications": total_publications,
        "domestic_percentage": round((total_domestic / total_publications) * 100, 2) if total_publications > 0 else 0,
        "international_percentage": round((total_international / total_publications) * 100, 2) if total_publications > 0 else 0,
    }
    
    total_row_df = pd.DataFrame([total_row_dict])
    
    if summary_df.empty:
        return total_row_df
    return pd.concat([summary_df, total_row_df], ignore_index=True)

def calculate_detailed_metrics_for_group(group_series: pd.Series) -> pd.Series:
    """
    Helper function to calculate all detailed citation metrics for a given Series of citation counts.
    Handles cases with empty or single-item series for standard deviation.
    """
    if group_series.empty:
        return pd.Series({
            "average_citations": np.nan,
            "std_dev_citations": np.nan,
            "median_citations": np.nan,
            "q1_citations": np.nan,
            "q3_citations": np.nan,
            "max_citations": np.nan,
            "percentage_uncited_papers": np.nan,
            "percentage_low_cited_papers": np.nan,
            "publication_count": 0
        })

    metrics = {
        "average_citations": group_series.mean(),
        "std_dev_citations": group_series.std() if len(group_series) > 1 else 0, # Std dev is 0 for single item, NaN if ddof=1
        "median_citations": group_series.median(),
        "q1_citations": group_series.quantile(0.25),
        "q3_citations": group_series.quantile(0.75),
        "max_citations": group_series.max(),
        "percentage_uncited_papers": (group_series == 0).mean() * 100,
        "percentage_low_cited_papers": (group_series.between(1, 5, inclusive='both')).mean() * 100,
        "publication_count": len(group_series)
    }
    return pd.Series(metrics)

def generate_detailed_citation_metrics_by_subfield(processed_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates detailed citation metrics for each subfield, broken down by
    domestic and international collaboration. Also includes overall totals.
    This data is for the second output table.
    """
    if processed_df.empty:
        print("Warning: Processed DataFrame is empty. Cannot generate detailed metrics.")
        return pd.DataFrame()

    # Calculate metrics per subfield and collaboration type
    detailed_metrics_subfield = (
        processed_df.groupby(["subfield_display", "is_domestic"])["cited_by_count"]
        .apply(calculate_detailed_metrics_for_group)
        .unstack(level=-1) # Unstack the results of apply if it returns a Series
    )
     # If apply returns a DataFrame, unstacking might not be needed or might need adjustment
    if isinstance(detailed_metrics_subfield.columns, pd.MultiIndex):
         detailed_metrics_subfield.columns = ['_'.join(map(str,col)).strip() for col in detailed_metrics_subfield.columns.values]


    detailed_metrics_subfield = detailed_metrics_subfield.reset_index()
    
    # The above groupby().apply().unstack() can be tricky.
    # Let's try a more direct approach for clarity and robustness:
    
    all_metrics_list = []

    # Metrics per subfield
    for (subfield, is_dom), group in processed_df.groupby(["subfield_display", "is_domestic"]):
        metrics = calculate_detailed_metrics_for_group(group["cited_by_count"])
        metrics['subfield_display'] = subfield
        metrics['collaboration_type'] = 'Domestic' if is_dom else 'International'
        all_metrics_list.append(metrics)
    
    # Overall metrics (Total)
    for is_dom, group in processed_df.groupby("is_domestic"):
        metrics = calculate_detailed_metrics_for_group(group["cited_by_count"])
        metrics['subfield_display'] = 'Total'
        metrics['collaboration_type'] = 'Domestic' if is_dom else 'International'
        all_metrics_list.append(metrics)
        
    if not all_metrics_list:
        return pd.DataFrame()

    final_detailed_metrics_df = pd.DataFrame(all_metrics_list)
    
    # Reorder columns for desired output
    column_order = [
        "subfield_display", "collaboration_type", "average_citations", "std_dev_citations",
        "median_citations", "q1_citations", "q3_citations", "max_citations",
        "percentage_uncited_papers", "percentage_low_cited_papers", "publication_count"
    ]
    final_detailed_metrics_df = final_detailed_metrics_df[column_order]

    # Sort results: Subfields alphabetically, then Total. Within each, Domestic then International.
    final_detailed_metrics_df['sort_subfield'] = final_detailed_metrics_df['subfield_display'].apply(lambda x: ('ZZZ' if x == 'Total' else x))
    final_detailed_metrics_df['sort_collab'] = final_detailed_metrics_df['collaboration_type'].apply(lambda x: 0 if x == 'Domestic' else 1)
    final_detailed_metrics_df = final_detailed_metrics_df.sort_values(by=['sort_subfield', 'sort_collab']).drop(columns=['sort_subfield', 'sort_collab'])

    # Rounding
    cols_to_round_2_decimals = ["average_citations", "std_dev_citations", "median_citations", 
                                "q1_citations", "q3_citations", 
                                "percentage_uncited_papers", "percentage_low_cited_papers"]
    for col in cols_to_round_2_decimals:
        final_detailed_metrics_df[col] = final_detailed_metrics_df[col].round(2)
    
    final_detailed_metrics_df["max_citations"] = final_detailed_metrics_df["max_citations"].astype('Int64') # Allow NaNs if any
    final_detailed_metrics_df["publication_count"] = final_detailed_metrics_df["publication_count"].astype(int)

    return final_detailed_metrics_df.reset_index(drop=True)


# Main execution flow
if __name__ == "__main__":
    print("Starting script...")
    print(f"Reading publications from: {PUBLICATIONS_PATH}")
    try:
        publications_df = pd.read_csv(PUBLICATIONS_PATH, low_memory=False) # Added low_memory=False for potential mixed types
        print(f"Successfully read {len(publications_df)} publications.")
    except FileNotFoundError:
        print(f"Error: The file {PUBLICATIONS_PATH} was not found.")
        exit()
    except Exception as e:
        print(f"Error reading {PUBLICATIONS_PATH}: {e}")
        exit()

    print("Processing base data (parsing subfields, determining domestic/international)...")
    processed_df = process_base_data(publications_df)
    if processed_df.empty:
        print("Critical Error: No data after processing. Exiting.")
        exit()
    print(f"Base data processed. {len(processed_df)} publications remaining after cleaning.")
    print(f"  Domestic publications count: {processed_df['is_domestic'].sum()}")
    print(f"  International publications count: {(~processed_df['is_domestic']).sum()}")

    # --- Generate and save the first summary file (proportions) ---
    print(f"\nGenerating publication proportions summary (for {OUTPUT_PATH_PROPORTIONS})...")
    proportions_summary_df = summarize_publication_proportions(processed_df)
    
    if not proportions_summary_df.empty:
        proportions_summary_df.sort_values(by="domestic_percentage", inplace=True, na_position='last')
    else:
        print("Warning: Proportions summary DataFrame is empty before adding total row.")
        
    final_proportions_summary = add_total_row_proportions(proportions_summary_df, processed_df)
    
    try:
        final_proportions_summary.to_csv(OUTPUT_PATH_PROPORTIONS, index=False)
        print(f"Proportions summary saved to {OUTPUT_PATH_PROPORTIONS}")
        print("Preview of proportions summary:")
        print(final_proportions_summary.head())
    except Exception as e:
        print(f"Error saving {OUTPUT_PATH_PROPORTIONS}: {e}")

    # --- Generate and save the second summary file (detailed metrics) ---
    print(f"\nGenerating detailed citation metrics by subfield (for {OUTPUT_PATH_DETAILED_METRICS})...")
    detailed_metrics_df = generate_detailed_citation_metrics_by_subfield(processed_df)
    
    if not detailed_metrics_df.empty:
        try:
            detailed_metrics_df.to_csv(OUTPUT_PATH_DETAILED_METRICS, index=False)
            print(f"Detailed citation metrics saved to {OUTPUT_PATH_DETAILED_METRICS}")
            print("Preview of detailed citation metrics (first few rows and Total):")
            print(pd.concat([detailed_metrics_df.head(), detailed_metrics_df[detailed_metrics_df['subfield_display'] == 'Total']]))
        except Exception as e:
            print(f"Error saving {OUTPUT_PATH_DETAILED_METRICS}: {e}")
    else:
        print("No detailed citation metrics were generated (DataFrame is empty).")

    print("\nScript finished.")