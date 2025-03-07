import json
import pandas as pd
import networkx as nx
from itertools import combinations
from collections import defaultdict
import os, sys
from utils import mappings

INPUT_PATH = "../../data/raw/publication_meta/br_publication_meta.csv"
OUTPUT_PATH = "../../data/graphs/years"

def parse_json_field(field_str):
    """
    Safely parse a JSON-formatted string.
    Returns a Python object (list or dict) if parsing is successful;
    otherwise returns None.
    """
    try:
        return json.loads(field_str)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return None


def get_author_country(author):
    """
    Extracts the country of the author from their institution data.
    Returns the first country code if available, else 'Unknown'.
    """
    countries = author.get("countries", [])
    if countries and len(countries) > 0:
        return countries[0]
    return "Unknown"


def update_author_subfield_counts(author_subfield_counts, authors, subfield_name):
    """
    Updates the count of publications for each author in a given subfield.
    Parameters:
      author_subfield_counts: dict mapping author id to a dict of subfield counts.
      authors: list of author dictionaries from a publication.
      subfield_name: the subfield (string) of the publication.
    """
    for author in authors:
        author_id = author.get("id")
        if author_id:
            author_subfield_counts[author_id][subfield_name] += 1


def determine_primary_subfield(subfield_count):
    """
    Given a dictionary of subfield counts for an author,
    returns the subfield with the highest count.
    If there is a tie, one is selected arbitrarily.
    """
    if not subfield_count:
        return "Unknown"
    return max(subfield_count.items(), key=lambda x: x[1])[0]


def build_collaboration_network(df):
    """
    Processes the publications dataframe to create a collaboration network.

    For each publication:
      - Parses the authorship and subfield fields.
      - Updates the subfield counts per author.
      - Adds an edge (or updates its weight) between every pair of co-authors.

    Returns:
      - A NetworkX graph with nodes having attributes 'label1' (country)
        and 'label2' (primary subfield), and edges weighted by collaboration count.
    """
    # Dictionary to store author info (country and later primary subfield)
    author_info = {}
    # Dictionary to count subfields per author (author id -> {subfield: count})
    author_subfield_counts = defaultdict(lambda: defaultdict(int))
    # Dictionary to store collaboration edges with their weights (tuple(author1, author2) -> count)
    collaboration_edges = defaultdict(int)

    for idx, row in df.iterrows():
        # Parse the authorship field into a list of author dictionaries
        authors = parse_json_field(row["authorships"])
        if authors is None:
            continue

        # Parse the subfield information (assumed to be a JSON dict)
        subfield_data = parse_json_field(row["subfield"])
        subfield_name = (
            subfield_data.get("display_name", "Unknown") if subfield_data else "Unknown"
        )

        # Update author subfield counts and store basic country info
        for author in authors:
            author_id = author.get("id")
            if not author_id:
                continue
            # If we haven't seen this author, add their country info
            if author_id not in author_info:
                country = get_author_country(author)
                author_info[author_id] = {"country": country}
            # Update the subfield count for this author
            author_subfield_counts[author_id][subfield_name] += 1

        # Update collaboration counts for each pair of co-authors
        for author1, author2 in combinations(authors, 2):
            id1, id2 = author1.get("id"), author2.get("id")
            if id1 and id2:
                # Sort tuple so that edge key is order-independent (undirected graph)
                edge = tuple(sorted([id1, id2]))
                collaboration_edges[edge] += 1

    # Create the graph and add nodes with the appropriate attributes
    G = nx.Graph()
    for author_id, info in author_info.items():
        # Determine the primary subfield from the counts
        primary_subfield = determine_primary_subfield(author_subfield_counts[author_id])
        # label1: country, label2: primary subfield
        G.add_node(author_id, label1=info["country"], label2=primary_subfield)

    # Add weighted edges for collaborations
    for (id1, id2), weight in collaboration_edges.items():
        G.add_edge(id1, id2, weight=weight)

    return G


def filter_subfielf_publications(df: pd.DataFrame, subfiled: str) -> pd.DataFrame:
    """
    Filters the DataFrame to include only publications where the
    subfield's display_name is 'Artificial Intelligence'.

    The function assumes that the 'subfield' column contains a JSON string.
    """
    # Use a lambda to parse the JSON and check the display_name
    filtered_df = df[
        df["subfield"].apply(lambda x: json.loads(x).get("display_name") == subfiled)
    ]
    return filtered_df


def filter_publications_by_citation_count(
    df: pd.DataFrame, num_citations: int
) -> pd.DataFrame:
    """
    Filters the DataFrame to include only publications with more than the specified number of citations.
    """
    filtered_df = df[df["cited_by_count"] > num_citations]
    return filtered_df


def filter_publications_by_year(
    df: pd.DataFrame, year: int
) -> pd.DataFrame:
    """
    Filters the DataFrame to include only publications for a specific year.
    """
    filtered_df = df[df["publication_year"] == year]
    return filtered_df



def main():
    """
    Main function that loads the publications data, builds the collaboration network,
    and saves the network as a GEXF file.
    """
    # Load the CSV file into a pandas DataFrame.
    # Adjust the file name/path as needed.
    try:
        full_df = pd.read_csv(INPUT_PATH)
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    for subfield in mappings.SUBFIELDS_SHORT.keys():
        # Filter the DataFrame to include only a subfield of interest
        subfielf_df = filter_subfielf_publications(full_df, subfield)

        for year in range(2015, 2025):
            df = filter_publications_by_year(subfielf_df, year)

            # Build the collaboration network graph
            G = build_collaboration_network(df)

            # Write the graph to a GEXF file for visualization (e.g., in Gephi)
            output_file = str(f"{OUTPUT_PATH}/{subfield}_{year}.gexf")
            try:
                nx.write_gexf(G, output_file)
                print(f"Graph successfully written to {output_file}")
            except Exception as e:
                print(f"Error writing GEXF file: {e}")


if __name__ == "__main__":
    main()
