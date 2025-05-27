import pandas as pd
import json

def analyze_duplicates(file_path):
    """
    Analyze duplicates in the CSV file based on different criteria.
    
    Parameters:
    file_path (str): Path to the CSV file
    
    Returns:
    dict: Dictionary containing counts of different types of duplicates
    """
    # Read the CSV file
    print(f"Reading file from: {file_path}")
    try:
        # Try with different encodings if necessary
        df = pd.read_csv(file_path, low_memory=False)
    except UnicodeDecodeError:
        # If default encoding fails, try with utf-8
        df = pd.read_csv(file_path, encoding='utf-8', low_memory=False)
    
    print(f"Successfully loaded CSV with {len(df)} rows and {len(df.columns)} columns")
    print(f"Column names: {df.columns.tolist()}")
    
    # Count of duplicate entries (whole line)
    duplicate_rows = df.duplicated().sum()
    print(f"\nTotal number of duplicate rows (all columns): {duplicate_rows}")
    
    # Count of duplicated values considering only the column "id"
    id_duplicates = df.duplicated(subset=['id'], keep=False).sum()
    unique_ids_with_duplicates = df[df.duplicated(subset=['id'], keep=False)]['id'].nunique()
    print(f"\nTotal rows with duplicate 'id' values: {id_duplicates}")
    print(f"Number of unique 'id' values that appear multiple times: {unique_ids_with_duplicates}")
    
    # Count of duplicated values considering only the column "doi"
    doi_duplicates = df.duplicated(subset=['doi'], keep=False).sum()
    unique_dois_with_duplicates = df[df.duplicated(subset=['doi'], keep=False)]['doi'].nunique()
    print(f"\nTotal rows with duplicate 'doi' values: {doi_duplicates}")
    print(f"Number of unique 'doi' values that appear multiple times: {unique_dois_with_duplicates}")
    
    # Count of duplicated values considering the columns "id" and "doi"
    id_doi_duplicates = df.duplicated(subset=['id', 'doi'], keep=False).sum()
    unique_id_doi_pairs_with_duplicates = df[df.duplicated(subset=['id', 'doi'], keep=False)].groupby(['id', 'doi']).size().count()
    print(f"\nTotal rows with duplicate 'id' and 'doi' combinations: {id_doi_duplicates}")
    print(f"Number of unique 'id' and 'doi' combinations that appear multiple times: {unique_id_doi_pairs_with_duplicates}")
    
    # Additional analysis
    print("\nDetailed analysis:")
    
    # Show first few examples of duplicated IDs
    if id_duplicates > 0:
        duplicate_ids = df[df.duplicated(subset=['id'], keep=False)]['id'].unique()[:5]  # Show up to 5 examples
        print(f"\nExample duplicate IDs: {', '.join(map(str, duplicate_ids))}")
        
        # For the first duplicate ID, show its occurrences
        if len(duplicate_ids) > 0:
            first_dup_id = duplicate_ids[0]
            dup_rows = df[df['id'] == first_dup_id]
            print(f"\nSample rows for duplicate ID '{first_dup_id}':")
            print(dup_rows[['id', 'doi', 'title']].head(2).to_string(index=False))
    
    # Show first few examples of duplicated DOIs
    if doi_duplicates > 0:
        duplicate_dois = df[df.duplicated(subset=['doi'], keep=False)]['doi'].unique()[:5]  # Show up to 5 examples
        print(f"\nExample duplicate DOIs: {', '.join(map(str, duplicate_dois))}")
        
        # For the first duplicate DOI, show its occurrences
        if len(duplicate_dois) > 0 and pd.notna(duplicate_dois[0]):
            first_dup_doi = duplicate_dois[0]
            dup_rows = df[df['doi'] == first_dup_doi]
            print(f"\nSample rows for duplicate DOI '{first_dup_doi}':")
            print(dup_rows[['id', 'doi', 'title']].head(2).to_string(index=False))
    
    # Return the counts
    return {
        'duplicate_rows': duplicate_rows,
        'id_duplicates': id_duplicates,
        'unique_ids_with_duplicates': unique_ids_with_duplicates,
        'doi_duplicates': doi_duplicates,
        'unique_dois_with_duplicates': unique_dois_with_duplicates,
        'id_doi_duplicates': id_doi_duplicates,
        'unique_id_doi_pairs_with_duplicates': unique_id_doi_pairs_with_duplicates
    }

if __name__ == "__main__":
    FILE_PATH = "../../data/raw/publication_meta/br_publication_meta.csv"
    results = analyze_duplicates(FILE_PATH)
    
    # Print summary
    print("\n===== SUMMARY =====")
    print(f"Total duplicate rows (whole line): {results['duplicate_rows']}")
    print(f"Rows with duplicate 'id' values: {results['id_duplicates']}")
    print(f"Unique 'id' values with duplicates: {results['unique_ids_with_duplicates']}")
    print(f"Rows with duplicate 'doi' values: {results['doi_duplicates']}")
    print(f"Unique 'doi' values with duplicates: {results['unique_dois_with_duplicates']}")
    print(f"Rows with duplicate 'id' and 'doi' combinations: {results['id_doi_duplicates']}")
    print(f"Unique 'id' and 'doi' pairs with duplicates: {results['unique_id_doi_pairs_with_duplicates']}")