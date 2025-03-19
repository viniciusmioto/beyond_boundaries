import pandas as pd
import ast
from collections import defaultdict, Counter

INPUT_FILE = "../../data/raw/publication_meta/br_publication_meta.csv"
OUTPUT_FILE = "../../data/processed/6_authors.csv"

# Initialize data structure to store author information
authors = defaultdict(lambda: {
    'name': '',
    'countries': Counter(),
    'publications': []  # Store all publication details
})

# Read the CSV file
df = pd.read_csv(INPUT_FILE)

# Process each publication
for _, row in df.iterrows():
    try:
        # Parse relevant fields
        authorships = ast.literal_eval(row['authorships'])
        subfield = ast.literal_eval(row['subfield'])
        subfield_name = subfield['display_name']
        publication_year = int(row['publication_year'])
        cited_by_count = int(row['cited_by_count'])
        num_authors = len(authorships)
        
        # Update author information
        for author in authorships:
            author_id = author['id']
            author_name = author['name']
            countries = author['countries']
            
            # Update name (assuming consistency across entries)
            if not authors[author_id]['name']:
                authors[author_id]['name'] = author_name
                
            # Update country counts
            authors[author_id]['countries'].update(countries)
            
            # Store publication details
            authors[author_id]['publications'].append({
                'subfield': subfield_name,
                'year': publication_year,
                'cited_by': cited_by_count,
                'num_authors': num_authors
            })
            
    except (SyntaxError, ValueError, KeyError) as e:
        print(f"Error processing row {_}: {e}")

# Prepare data for DataFrame
rows = []
subfields = set()

# Collect all unique subfields
for data in authors.values():
    for pub in data['publications']:
        subfields.add(pub['subfield'])

subfields = sorted(subfields)

# Create rows for each author
for author_id, data in authors.items():
    # Get most common country
    country = data['countries'].most_common(1)
    country = country[0][0] if country else None
    
    # Calculate subfield counts
    subfield_counts = Counter(pub['subfield'] for pub in data['publications'])
    total = sum(subfield_counts.values())
    
    # Determine primary subfield with tie-breaking logic
    primary_subfield = None
    if subfield_counts:
        max_count = max(subfield_counts.values())
        candidates = [sub for sub, cnt in subfield_counts.items() if cnt == max_count]
        
        if len(candidates) == 1:
            primary_subfield = candidates[0]
        else:
            # Filter publications to candidate subfields
            candidate_pubs = [pub for pub in data['publications'] if pub['subfield'] in candidates]
            
            # Sort by: 1) year (desc), 2) citations (desc), 3) num authors (asc)
            candidate_pubs.sort(key=lambda x: (-x['year'], -x['cited_by'], x['num_authors']))
            
            if candidate_pubs:
                primary_subfield = candidate_pubs[0]['subfield']
    
    # Create row
    row = {
        'id': author_id,
        'name': data['name'],
        'country': country,
        'primary_subfield': primary_subfield,
        'total_publications': total,
    }
    
    # Add subfield counts
    for subfield in subfields:
        row[subfield] = subfield_counts.get(subfield, 0)
    
    rows.append(row)

# Create DataFrame
df_authors = pd.DataFrame(rows)

# Reorder columns
columns = ['id', 'name', 'country', 'primary_subfield', 'total_publications'] + sorted(subfields)
df_authors = df_authors[columns]

# Save to CSV
df_authors.to_csv(OUTPUT_FILE, index=False)

print("Authors CSV created successfully with tie-breaking logic!")