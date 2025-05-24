import pandas as pd
import json

def process_collab_data(br_publications_path, authors_path, output_path):
    # Load datasets
    br_publications = pd.read_csv(br_publications_path)
    print("BR Publications Data Loaded")
    authors = pd.read_csv(authors_path)
    print("Authors Data Loaded")

    # Function to determine collaboration type
    def get_subfield_collaboration(authorships_str):
        try:
            # Convert to valid JSON and parse
            author_list = json.loads(authorships_str.replace("'", '"'))
            author_ids = [a['id'] for a in author_list]
            
            # Get unique subfields from authors
            subfields = authors.loc[authors['id'].isin(author_ids), 'primary_subfield'].unique()
            
            return 'Multi-Subfield' if len(subfields) > 1 else 'Single-Subfield'
        except Exception:
            return 'Unknown'

    # Add collaboration type column
    br_publications['collab_type'] = br_publications['authorships'].apply(get_subfield_collaboration)
    print("Collaboration types determined")

    # Filter out publications with unknown collaboration type
    filtered_publications = br_publications[br_publications['collab_type'] != 'Unknown']
    print("Filtered publications with unknown collaboration type")

    # Save processed data
    filtered_publications.to_csv(output_path, index=False)
    print(f"Processed data saved to {output_path}")

if __name__ == "__main__":
    process_collab_data(
        br_publications_path="../../data/raw/publication_meta/br_publication_meta.csv",
        authors_path="../../data/processed/3_authors.csv",
        output_path="../../data/processed/6_publications_collab_types.csv"
    )