import pandas as pd

FILES_PATH = "../../data/raw/publication_meta/openalex_publications"
OUTPUT_FILE = "../../data/raw/publication_meta/br_publication_meta.csv"
PUBLICATION_YEAR = [
    "2024",
    "2023",
    "2022",
    "2021",
    "2020",
    "2019",
    "2018",
    "2017",
    "2016",
    "2015"
]

def deduplicate_strategy(dfs):
    """
    Apply a comprehensive deduplication strategy across multiple dataframes.
    
    Strategy:
    1. First combine all dataframes to see the full picture of duplicates
    2. Prioritize keeping the most complete/recent records when duplicates are found
    3. Remove exact duplicates first (all columns identical)
    4. Handle ID and DOI duplicates with a carefully designed strategy
    
    Parameters:
    dfs (list): List of pandas DataFrames to be deduplicated and combined
    
    Returns:
    pandas.DataFrame: Deduplicated combined DataFrame
    """
    # Concatenate all DataFrames first to see the complete picture
    print("Concatenating all yearly dataframes...")
    combined_df = pd.concat(dfs, ignore_index=True)
    initial_count = len(combined_df)
    print(f"Initial combined dataset has {initial_count} rows")
    
    # Step 1: Remove exact duplicates (all columns identical)
    print("\nRemoving exact duplicate rows (all columns identical)...")
    combined_df.drop_duplicates(inplace=True)
    after_exact_dedup_count = len(combined_df)
    print(f"After removing exact duplicates: {after_exact_dedup_count} rows")
    print(f"Removed {initial_count - after_exact_dedup_count} exact duplicate rows")
    
    # Step 2: Handle 'id' and 'doi' duplicates with a priority strategy
    # For academic publications, having the same DOI is a strong indicator of the same paper
    # The 'id' might be system-generated, but DOI is a standard identifier
    
    # First, fill NaN DOIs with empty string to simplify processing
    combined_df['doi'].fillna('', inplace=True)
    
    # Identify rows with duplicate IDs or DOIs
    duplicate_ids = combined_df.duplicated(subset=['id'], keep=False)
    duplicate_dois = combined_df[combined_df['doi'] != ''].duplicated(subset=['doi'], keep=False)
    
    print(f"\nFound {duplicate_ids.sum()} rows with duplicate 'id' values")
    print(f"Found {duplicate_dois.sum()} rows with duplicate 'doi' values (excluding empty DOIs)")
    
    # Create a helper column for sorting and prioritizing which duplicates to keep
    combined_df['dedup_priority'] = 0
    
    # Strategy: Prioritize rows based on:
    # 1. Having a non-empty DOI
    # 2. Having more citation data (cited_by_count)
    # 3. More recent data (higher publication_year)
    
    # Give priority to records with DOIs
    combined_df.loc[combined_df['doi'] != '', 'dedup_priority'] += 10
    
    # Give priority based on citation data presence
    combined_df['dedup_priority'] += combined_df['cited_by_count'].notna().astype(int) * 5
    
    # Give priority based on citation count (normalize to 0-3 range to avoid overwhelming other factors)
    max_cite = combined_df['cited_by_count'].max() if combined_df['cited_by_count'].max() > 0 else 1
    combined_df['dedup_priority'] += (combined_df['cited_by_count'].fillna(0) / max_cite * 3)
    
    # Give slight priority to more recent publications
    # Normalize publication year to 0-1 range within our dataset
    years = pd.to_numeric(combined_df['publication_year'], errors='coerce').fillna(0)
    min_year = years.min() if years.min() > 0 else 2015
    max_year = years.max() if years.max() > 0 else 2024
    year_range = max_year - min_year if max_year > min_year else 1
    combined_df['dedup_priority'] += ((years - min_year) / year_range)
    
    # Sort by dedup_priority (descending) to keep the best record first
    combined_df.sort_values('dedup_priority', ascending=False, inplace=True)
    
    # Now perform the deduplication, keeping the first occurrence (highest priority)
    print("\nRemoving duplicate 'id' values, keeping the highest quality record...")
    combined_df_dedup_id = combined_df.drop_duplicates(subset=['id'], keep='first')
    after_id_dedup_count = len(combined_df_dedup_id)
    print(f"After 'id' deduplication: {after_id_dedup_count} rows")
    print(f"Removed {after_exact_dedup_count - after_id_dedup_count} rows with duplicate 'id' values")
    
    # Now deduplicate based on DOI, but only for non-empty DOIs
    print("\nRemoving duplicate 'doi' values (where DOI is not empty)...")
    # Create a mask for rows with non-empty DOIs
    non_empty_doi_mask = combined_df_dedup_id['doi'] != ''
    
    # Split into dataframes with and without DOIs
    df_with_doi = combined_df_dedup_id[non_empty_doi_mask]
    df_without_doi = combined_df_dedup_id[~non_empty_doi_mask]
    
    # Deduplicate the DOI dataframe
    df_with_doi_dedup = df_with_doi.drop_duplicates(subset=['doi'], keep='first')
    
    # Recombine the dataframes
    final_df = pd.concat([df_with_doi_dedup, df_without_doi], ignore_index=True)
    final_count = len(final_df)
    print(f"After 'doi' deduplication: {final_count} rows")
    print(f"Removed {after_id_dedup_count - final_count} rows with duplicate 'doi' values")
    
    # Remove the helper column
    final_df.drop('dedup_priority', axis=1, inplace=True)
    
    # Finally, sort by publication year (descending) for better organization
    final_df.sort_values('publication_year', ascending=False, inplace=True, na_position='last')
    
    print(f"\nTotal rows removed: {initial_count - final_count}")
    print(f"Final deduplicated dataset has {final_count} rows")
    
    return final_df

def main():
    # Load and store each year's DataFrame in a list
    print(f"Loading data files for years {', '.join(PUBLICATION_YEAR)}...")
    dfs = []
    for year in PUBLICATION_YEAR:
        file_path = f"{FILES_PATH}_{year}.csv"
        print(f"Loading {file_path}...")
        try:
            df = pd.read_csv(file_path)
            print(f"Loaded {len(df)} rows for year {year}")
            dfs.append(df)
        except Exception as e:
            print(f"Error loading file for year {year}: {e}")
    
    # Apply deduplication strategy
    deduplicated_df = deduplicate_strategy(dfs)
    
    # Save the deduplicated DataFrame to a new CSV file
    print(f"\nSaving deduplicated dataset to {OUTPUT_FILE}...")
    deduplicated_df.to_csv(OUTPUT_FILE, index=False)
    print(f"Deduplicated dataset saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()