import pandas as pd
import json
import numpy as np
import csv
import sys
from pathlib import Path

# Increase CSV field size limit to handle large JSON strings
csv.field_size_limit(sys.maxsize)

def parse_authorships(authorships_str):
    """Parse the authorships JSON string and return the number of authors."""
    try:
        if pd.isna(authorships_str) or authorships_str == '':
            return 0
        authorships = json.loads(authorships_str)
        return len(authorships) if isinstance(authorships, list) else 0
    except (json.JSONDecodeError, TypeError) as e:
        # If we have exactly 100 authors, this might be a truncation issue
        if isinstance(authorships_str, str) and authorships_str.count('"id":') == 100:
            print(f"Warning: Potential truncation detected - found exactly 100 author IDs")
            print(f"String ends with: ...{authorships_str[-100:]}")
        return 0

def parse_subfield(subfield_str):
    """Parse the subfield JSON string and return the display name."""
    try:
        if pd.isna(subfield_str) or subfield_str == '':
            return None
        subfield = json.loads(subfield_str)
        return subfield.get('display_name', None) if isinstance(subfield, dict) else None
    except (json.JSONDecodeError, TypeError):
        return None

def calculate_subfield_metrics(df):
    """Calculate author metrics for each subfield."""
    results = []
    
    # Group by subfield
    for subfield_name, group in df.groupby('subfield_name'):
        if pd.isna(subfield_name):
            continue
            
        author_counts = group['author_count']
        
        # Calculate metrics
        avg_authors = author_counts.mean()
        std_dev_authors = author_counts.std()
        median_authors = author_counts.median()
        max_authors = author_counts.max()
        
        # Calculate percentage of single author publications
        single_author_count = (author_counts == 1).sum()
        total_publications = len(author_counts)
        percentage_single_author = (single_author_count / total_publications) * 100 if total_publications > 0 else 0
        
        results.append({
            'subfield_name': subfield_name,
            'avg_authors_per_publication': round(avg_authors, 2),
            'std_dev_authors_per_publication': round(std_dev_authors, 2),
            'median_authors_per_publication': int(median_authors),
            'max_authors_in_publication': int(max_authors),
            'percentage_single_author_publications': round(percentage_single_author, 2)
        })
    
    return results

def calculate_total_metrics(df):
    """Calculate overall metrics for all publications."""
    author_counts = df['author_count']
    
    avg_authors = author_counts.mean()
    std_dev_authors = author_counts.std()
    median_authors = author_counts.median()
    max_authors = author_counts.max()
    
    # Calculate percentage of single author publications
    single_author_count = (author_counts == 1).sum()
    total_publications = len(author_counts)
    percentage_single_author = (single_author_count / total_publications) * 100 if total_publications > 0 else 0
    
    return {
        'subfield_name': 'TOTAL',
        'avg_authors_per_publication': round(avg_authors, 2),
        'std_dev_authors_per_publication': round(std_dev_authors, 2),
        'median_authors_per_publication': int(median_authors),
        'max_authors_in_publication': int(max_authors),
        'percentage_single_author_publications': round(percentage_single_author, 2)
    }

def main():
    # Define file paths
    INPUT_PATH = "../../data/raw/publication_meta/br_publication_meta.csv"
    OUTPUT_PATH = "../../data/processed/12_subfield_authors_metrics.csv"
    
    try:
        # Read the CSV file with increased field size limit
        print("Reading CSV file...")
        df = pd.read_csv(INPUT_PATH)
        print(f"Loaded {len(df)} publications")
        
        # Parse authorships and subfields
        print("Parsing authorships and subfields...")
        df['author_count'] = df['authorships'].apply(parse_authorships)
        df['subfield_name'] = df['subfield'].apply(parse_subfield)
        
        # Debug: Show some statistics about author counts
        print(f"Author count statistics:")
        print(f"  Min authors: {df['author_count'].min()}")
        print(f"  Max authors: {df['author_count'].max()}")
        print(f"  Mean authors: {df['author_count'].mean():.2f}")
        print(f"  Publications with >100 authors: {(df['author_count'] > 100).sum()}")
        print(f"  Publications with exactly 100 authors: {(df['author_count'] == 100).sum()}")
        
        # Debug: Check if any authorships strings end abruptly (indicating truncation)
        publications_with_100 = df[df['author_count'] == 100]
        if len(publications_with_100) > 0:
            print(f"\nDebugging publications with exactly 100 authors:")
            for idx, row in publications_with_100.head(3).iterrows():
                auth_str = row['authorships']
                print(f"  Publication {row.get('id', idx)}:")
                print(f"    Authorships string length: {len(auth_str) if isinstance(auth_str, str) else 'N/A'}")
                print(f"    String ends with: ...{auth_str[-50:] if isinstance(auth_str, str) else 'N/A'}")
                try:
                    parsed_authors = json.loads(auth_str)
                    actual_count = len(parsed_authors) if isinstance(parsed_authors, list) else 0
                    print(f"    Is valid JSON: True")
                    print(f"    Actual author count from JSON: {actual_count}")
                    # Check if there might be more authors in the data source
                    if actual_count == 100:
                        print(f"    This publication legitimately has exactly 100 authors")
                except Exception as e:
                    print(f"    Is valid JSON: False - {str(e)}")
        
        # Filter out publications without valid subfield or author data
        valid_df = df[(df['subfield_name'].notna()) & (df['author_count'] > 0)]
        print(f"Found {len(valid_df)} publications with valid subfield and author data")
        
        if len(valid_df) == 0:
            print("No valid data found. Please check the input file format.")
            return
        
        # Calculate metrics for each subfield
        print("Calculating subfield metrics...")
        subfield_results = calculate_subfield_metrics(valid_df)
        
        # Calculate total metrics
        print("Calculating total metrics...")
        total_result = calculate_total_metrics(valid_df)
        
        # Combine results
        all_results = subfield_results + [total_result]
        
        # Create DataFrame and save to CSV
        results_df = pd.DataFrame(all_results)
        
        # Ensure output directory exists
        output_path = Path(OUTPUT_PATH)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save results
        results_df.to_csv(OUTPUT_PATH, index=False)
        print(f"Results saved to {OUTPUT_PATH}")
        
        # Display summary
        print(f"\nAnalysis complete!")
        print(f"Total subfields analyzed: {len(subfield_results)}")
        print(f"Total publications processed: {len(valid_df)}")
        
        # Show first few results
        print("\nFirst few results:")
        print(results_df.head())
        
    except FileNotFoundError:
        print(f"Error: Could not find input file at {INPUT_PATH}")
        print("Please ensure the file exists and the path is correct.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        print("Please check your data format and try again.")

if __name__ == "__main__":
    main()