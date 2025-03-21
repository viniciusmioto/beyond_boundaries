import pandas as pd
import numpy as np

# Load the data
INPUT_PATH = "../../data/processed/6_authors.csv"
df = pd.read_csv(INPUT_PATH)

# Get all subfield columns (excluding metadata columns)
subfield_columns = df.columns[5:]

# Dictionary to store collaboration counts
collab_counts = {}

# For each author, find their primary subfield and other subfields they published in
for _, row in df.iterrows():
    primary_field = row['primary_subfield']
    
    # Skip if primary field is missing
    if pd.isna(primary_field):
        continue
    
    # Get other subfields this author has published in
    other_subfields = []
    for subfield in subfield_columns:
        # Skip the author's primary subfield
        if subfield == primary_field:
            continue
        
        # Add subfield if there are publications
        if row[subfield] > 0:
            other_subfields.append((subfield, row[subfield]))
    
    # Sort collaborations by publication count (descending)
    other_subfields.sort(key=lambda x: x[1], reverse=True)
    
    # Add to the count for this primary field
    if primary_field not in collab_counts:
        collab_counts[primary_field] = {}
    
    # Count collaborations
    for subfield, _ in other_subfields:
        if subfield not in collab_counts[primary_field]:
            collab_counts[primary_field][subfield] = 0
        collab_counts[primary_field][subfield] += 1

# Create result dataframe
results = []
for primary, collabs in collab_counts.items():
    # Sort collaborations by frequency
    sorted_collabs = sorted(collabs.items(), key=lambda x: x[1], reverse=True)
    
    # Get top 3 (or fewer if less than 3 exist)
    top_collabs = sorted_collabs[:3]
    
    # Pad with None if less than 3
    while len(top_collabs) < 3:
        top_collabs.append((None, 0))
    
    results.append({
        'Primary Subfield': primary,
        '1st Most Collaborative Subfield': top_collabs[0][0],
        '2nd Most Collaborative Subfield': top_collabs[1][0],
        '3rd Most Collaborative Subfield': top_collabs[2][0]
    })

# Create and save the results dataframe
result_df = pd.DataFrame(results)
OUTPUT_PATH = "../../data/processed/9_subfield_collabs.csv"
result_df.to_csv(OUTPUT_PATH, index=False)

print(f"Results saved to {OUTPUT_PATH}")