import pandas as pd
import numpy as np
from utils import mappings  # Import the mapping dictionary

# Load the data
INPUT_PATH = "../../data/processed/6_authors.csv"
df = pd.read_csv(INPUT_PATH)

# Get all subfield columns (excluding metadata columns)
subfield_columns = df.columns[5:]

# Dictionaries to store collaboration counts and author counts per primary field
collab_counts = {}
total_authors = {}
collab_authors = {}

# For each author, find their primary subfield and other subfields they published in
for _, row in df.iterrows():
    primary_field = row["primary_subfield"]

    # Skip if primary field is missing
    if pd.isna(primary_field):
        continue

    # Update total authors for the primary field
    total_authors[primary_field] = total_authors.get(primary_field, 0) + 1

    # Get other subfields this author has published in
    other_subfields = []
    for subfield in subfield_columns:
        # Skip the author's primary subfield
        if subfield == primary_field:
            continue

        # Add subfield if there are publications
        if row[subfield] > 0:
            other_subfields.append((subfield, row[subfield]))

    # If the author published in any other subfield, count them as a collaborator
    if other_subfields:
        collab_authors[primary_field] = collab_authors.get(primary_field, 0) + 1

    # Sort collaborations by publication count (descending)
    other_subfields.sort(key=lambda x: x[1], reverse=True)

    # Initialize the dictionary for this primary field if necessary
    if primary_field not in collab_counts:
        collab_counts[primary_field] = {}

    # Count collaborations
    for subfield, _ in other_subfields:
        if subfield not in collab_counts[primary_field]:
            collab_counts[primary_field][subfield] = 0
        collab_counts[primary_field][subfield] += 1

# Create result dataframe with top three collaborative subfields and collaboration percentage
results = []
for primary, collabs in collab_counts.items():
    # Sort collaborations by frequency
    sorted_collabs = sorted(collabs.items(), key=lambda x: x[1], reverse=True)

    # Get top 3 (or fewer if less than 3 exist)
    top_collabs = sorted_collabs[:3]

    # Pad with None if less than 3
    while len(top_collabs) < 3:
        top_collabs.append((None, 0))

    # Calculate the percentage of authors with collaborations for the primary field
    total = total_authors.get(primary, 0)
    collaborators = collab_authors.get(primary, 0)
    if total > 0:
        percentage = (collaborators / total) * 100
    else:
        percentage = 0

    # Map full subfield names to short names using mappings.SUBFIELDS_SHORT
    short_primary = mappings.SUBFIELDS_SHORT.get(primary, primary)
    short_collab1 = (
        mappings.SUBFIELDS_SHORT.get(top_collabs[0][0], top_collabs[0][0])
        if top_collabs[0][0] is not None
        else None
    )
    short_collab2 = (
        mappings.SUBFIELDS_SHORT.get(top_collabs[1][0], top_collabs[1][0])
        if top_collabs[1][0] is not None
        else None
    )
    short_collab3 = (
        mappings.SUBFIELDS_SHORT.get(top_collabs[2][0], top_collabs[2][0])
        if top_collabs[2][0] is not None
        else None
    )

    results.append(
        {
            "Primary Subfield": short_primary,
            "1st Most Collaborative Subfield": short_collab1,
            "2nd Most Collaborative Subfield": short_collab2,
            "3rd Most Collaborative Subfield": short_collab3,
            "Collaboration Percentage": round(percentage, 2),
        }
    )

# Create and save the results dataframe
result_df = pd.DataFrame(results).sort_values(
    by="Collaboration Percentage", ascending=False
)
OUTPUT_PATH = "../../data/processed/9_subfield_collabs.csv"
result_df.to_csv(OUTPUT_PATH, index=False)

print(f"Results saved to {OUTPUT_PATH}")
