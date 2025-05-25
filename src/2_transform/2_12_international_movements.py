import pandas as pd
import ast
from collections import defaultdict

INPUT_FILE = "../../data/raw/publication_meta/br_publication_meta.csv"
DETAILED_OUTPUT_FILE = "institutional_movements_details.csv"
YEARLY_OUTPUT_FILE = "../../data/processed/international_movements.csv"

def analyze_institutional_movements(input_file, detailed_output_file="institutional_movements_details.csv", yearly_output_file="../../data/processed/11_international_movements.csv"):
    """
    Analyze institutional movements of authors between Brazilian and foreign institutions,
    generate a detailed output file, and compute yearly movements.
    
    Args:
        input_file (str): Path to the CSV file containing publication metadata.
        detailed_output_file (str): Path to the CSV file where detailed movement data will be saved.
        yearly_output_file (str): Path to the CSV file where yearly movement data will be saved.
    
    Returns:
        tuple: (foreign_to_brazilian_count, brazilian_to_foreign_count)
    """
    
    # Read the CSV file
    print("Reading publication data...")
    df = pd.read_csv(input_file)
    
    # Data structure to store each author's chronological institutional history
    # Key: author_id, Value: list of (year, institutions, countries, publication_id) tuples
    author_histories = defaultdict(list)
    
    # Process each publication to build author histories
    print("Processing publications and building author histories...")
    processed_count = 0
    error_count = 0
    
    for _, row in df.iterrows():
        try:
            # Parse the authorships and publication year and ID
            authorships = ast.literal_eval(row['authorships'])
            publication_year = int(row['publication_year'])
            publication_id = row['id'] # Get the publication ID
            
            # For each author in this publication
            for author in authorships:
                author_id = author['id']
                author_name = author['name']
                
                # Extract institution countries for this author in this publication
                institutions = author.get('institutions', [])
                countries = set()
                
                # Collect all countries from the author's institutions in this publication
                for institution in institutions:
                    # Some institutions might not have country info, so we check
                    if 'country' in institution and institution['country']: # Ensure country is not empty
                        countries.add(institution['country'])
                
                # Also check the direct 'countries' field as shown in your example
                if 'countries' in author and author['countries']:
                    # Ensure each country in the list is not empty
                    countries.update([c for c in author['countries'] if c])
                
                # Only add if we have valid country information for this publication
                if countries: 
                    author_histories[author_id].append({
                        'year': publication_year,
                        'countries': countries,
                        'name': author_name,
                        'publication_id': publication_id # Store publication ID
                    })
            
            processed_count += 1
            
        except (SyntaxError, ValueError, KeyError, TypeError) as e:
            error_count += 1
            continue
    
    print(f"Processed {processed_count} publications successfully")
    if error_count > 0:
        print(f"Encountered {error_count} errors while processing")
    
    # Now analyze movements for each author
    print("Analyzing institutional movements...")
    
    # Counters for the two types of movements we're tracking
    foreign_to_brazilian_movements = set()  # Using set to avoid counting same author multiple times
    brazilian_to_foreign_movements = set()  # Using set to avoid counting same author multiple times
    
    # Dictionaries to store yearly movement counts (using sets for unique authors per year)
    yearly_foreign_to_brazilian = defaultdict(set)
    yearly_brazilian_to_foreign = defaultdict(set)

    # List to store detailed movement records
    detailed_movements_data = []
    
    total_authors = len(author_histories)
    authors_with_movements = 0
    
    for author_id, history in author_histories.items():
        # Skip authors with only one publication or no valid country data (no movement possible)
        if len(history) < 2:
            continue
        
        # Sort publications by year to get chronological order
        history.sort(key=lambda x: x['year'])
        
        # Track institutional affiliations over time
        previous_has_brazilian = None
        author_has_movement = False
        
        for i, publication in enumerate(history):
            current_countries = publication['countries']
            
            # Skip publications with no valid country information
            if not current_countries:
                continue

            current_has_brazilian = 'BR' in current_countries
            
            # If this is the first valid publication for comparison
            if previous_has_brazilian is None:
                previous_has_brazilian = current_has_brazilian
                continue
            
            # Get previous publication details
            prev_publication = history[i-1]
            prev_countries = prev_publication['countries']
            prev_year = prev_publication['year']
            prev_publication_id = prev_publication['publication_id']
            prev_author_name = prev_publication['name'] # Get author name from previous publication

            # Detect movements only when there's a clear transition AND previous countries were not empty
            # Movement 1: Foreign to Brazilian (author was only in foreign institutions, now has Brazilian)
            if not previous_has_brazilian and current_has_brazilian:
                foreign_to_brazilian_movements.add(author_id)
                author_has_movement = True
                yearly_foreign_to_brazilian[publication['year']].add(author_id) # Track yearly movement
                detailed_movements_data.append({
                    'author_id': author_id,
                    'author_name': prev_author_name,
                    'movement_type': 'Foreign to Brazilian',
                    'prev_year': prev_year,
                    'prev_countries': ", ".join(sorted(list(prev_countries))),
                    'prev_publication_id': prev_publication_id,
                    'curr_year': publication['year'],
                    'curr_countries': ", ".join(sorted(list(current_countries))),
                    'curr_publication_id': publication['publication_id']
                })
            
            # Movement 2: Brazilian to Foreign (author had Brazilian institutions, now only foreign)
            elif previous_has_brazilian and not current_has_brazilian:
                brazilian_to_foreign_movements.add(author_id)
                author_has_movement = True
                yearly_brazilian_to_foreign[publication['year']].add(author_id) # Track yearly movement
                detailed_movements_data.append({
                    'author_id': author_id,
                    'author_name': prev_author_name,
                    'movement_type': 'Brazilian to Foreign',
                    'prev_year': prev_year,
                    'prev_countries': ", ".join(sorted(list(prev_countries))),
                    'prev_publication_id': prev_publication_id,
                    'curr_year': publication['year'],
                    'curr_countries': ", ".join(sorted(list(current_countries))),
                    'curr_publication_id': publication['publication_id']
                })
            
            # Update previous state for next iteration only if current publication has valid countries
            previous_has_brazilian = current_has_brazilian
        
        if author_has_movement:
            authors_with_movements += 1
    
    # Create a DataFrame from the detailed movement data
    detailed_movements_df = pd.DataFrame(detailed_movements_data)
    
    # Save the detailed movements to a CSV file
    try:
        detailed_movements_df.to_csv(detailed_output_file, index=False)
        print(f"Detailed movement data saved to {detailed_output_file}")
    except Exception as e:
        print(f"Error saving detailed movement data to CSV: {e}")

    # Prepare yearly movement data for CSV
    yearly_data = []
    all_years = sorted(list(set(yearly_foreign_to_brazilian.keys()).union(yearly_brazilian_to_foreign.keys())))
    
    for year in all_years:
        yearly_data.append({
            'year': year,
            'foreign_to_brazilian_movements': len(yearly_foreign_to_brazilian[year]),
            'brazilian_to_foreign_movements': len(yearly_brazilian_to_foreign[year])
        })
    
    yearly_movements_df = pd.DataFrame(yearly_data)
    
    # Save yearly movements to a CSV file
    try:
        yearly_movements_df.to_csv(yearly_output_file, index=False)
        print(f"Yearly movement data saved to {yearly_output_file}")
    except Exception as e:
        print(f"Error saving yearly movement data to CSV: {e}")

    # Print overall results
    print(f"\n=== INSTITUTIONAL MOVEMENT ANALYSIS ===")
    print(f"Total authors analyzed: {total_authors}")
    print(f"Authors with institutional movements: {authors_with_movements}")
    print(f"\n=== MOVEMENT COUNTS ===")
    print(f"Authors who moved FROM foreign institutions TO Brazilian institutions: {len(foreign_to_brazilian_movements)}")
    print(f"Authors who moved FROM Brazilian institutions TO foreign institutions: {len(brazilian_to_foreign_movements)}")
    
    # Additional insights
    overlap = foreign_to_brazilian_movements.intersection(brazilian_to_foreign_movements)
    if overlap:
        print(f"\nNote: {len(overlap)} authors made both types of movements during their careers")
    
    return len(foreign_to_brazilian_movements), len(brazilian_to_foreign_movements)

# Main execution
if __name__ == "__main__":    
    try:
        foreign_to_br, br_to_foreign = analyze_institutional_movements(INPUT_FILE, DETAILED_OUTPUT_FILE, YEARLY_OUTPUT_FILE)
        
        print(f"\n=== SUMMARY ===")
        print(f"Inward movement (Foreign → Brazilian): {foreign_to_br} authors")
        print(f"Outward movement (Brazilian → Foreign): {br_to_foreign} authors")
        
    except FileNotFoundError:
        print(f"Error: Could not find the input file: {INPUT_FILE}")
        print("Please ensure the file path is correct.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")