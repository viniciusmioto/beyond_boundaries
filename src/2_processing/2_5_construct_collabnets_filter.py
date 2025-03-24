import json
import pandas as pd
import networkx as nx
from itertools import combinations
from collections import defaultdict
import os, sys
from utils import mappings

INPUT_PATH = "../../data/raw/publication_meta/br_publication_meta.csv"
OUTPUT_PATH = "../../data/graphs"
MIN_CITATIONS = 40
MIN_WEIGHT = 2


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


def determine_primary_subfield(subfield_count):
    """
    Given a dictionary of subfield counts for an author,
    returns the subfield with the highest count.
    If there is a tie, one is selected arbitrarily.
    """
    if not subfield_count:
        return "Unknown"
    return max(subfield_count.items(), key=lambda x: x[1])[0]


def calculate_author_subfield_counts(df):
    """
    Processes the publications dataframe to calculate subfield counts for each author.

    Parameters:
      df: DataFrame containing publication data with authorships and subfield columns.

    Returns:
      - Dictionary mapping author IDs to their subfield counts
      - Dictionary mapping author IDs to their countries
      - Dictionary mapping author IDs to their total publication counts
    """
    # Dictionary to store author info (country)
    author_info = {}
    # Dictionary to count subfields per author (author id -> {subfield: count})
    author_subfield_counts = defaultdict(lambda: defaultdict(int))
    # Dictionary to count publications per author (author id -> count)
    author_publication_counts = defaultdict(int)

    for idx, row in df.iterrows():
        # Parse the authorship field into a list of author dictionaries
        authors = parse_json_field(row["authorships"])
        if authors is None:
            continue

        # Parse the subfield information
        subfield_data = parse_json_field(row["subfield"])
        subfield_name = (
            subfield_data.get("display_name", "Unknown") if subfield_data else "Unknown"
        )

        # Update author subfield counts, publication counts, and store basic country info
        for author in authors:
            author_id = author.get("id")
            if not author_id:
                continue
            # If we haven't seen this author, add their country info
            if author_id not in author_info:
                country = get_author_country(author)
                author_info[author_id] = country
            # Update the subfield count for this author
            author_subfield_counts[author_id][subfield_name] += 1
            # Update the publication count for this author
            author_publication_counts[author_id] += 1

    return author_subfield_counts, author_info, author_publication_counts


def build_collaboration_network(
    df, author_subfield_counts, author_countries, author_publication_counts
):
    """
    Processes the publications dataframe to create a collaboration network.
    Uses pre-calculated subfield counts to determine primary subfields.

    Parameters:
      df: DataFrame containing publication data with authorships column.
      author_subfield_counts: Dictionary mapping author IDs to their subfield counts.
      author_countries: Dictionary mapping author IDs to their countries.
      author_publication_counts: Dictionary mapping author IDs to their publication counts.

    Returns:
      - A NetworkX graph with nodes having attributes 'label1' (country),
        'label2' (primary subfield), 'label3' (publication count), and
        edges weighted by collaboration count.
    """
    # Dictionary to store collaboration edges with their weights (tuple(author1, author2) -> count)
    collaboration_edges = defaultdict(int)

    # Track authors appearing in this filtered dataset
    authors_in_filtered_set = set()

    for idx, row in df.iterrows():
        # Parse the authorship field into a list of author dictionaries
        authors = parse_json_field(row["authorships"])
        if authors is None:
            continue

        # Track authors in this filtered dataset
        author_ids = [author.get("id") for author in authors if author.get("id")]
        authors_in_filtered_set.update(author_ids)

        # Update collaboration counts for each pair of co-authors
        for author1, author2 in combinations(authors, 2):
            id1, id2 = author1.get("id"), author2.get("id")
            if id1 and id2:
                # Sort tuple so that edge key is order-independent (undirected graph)
                edge = tuple(sorted([id1, id2]))
                collaboration_edges[edge] += 1

    # Create the graph and add nodes with the appropriate attributes
    G = nx.Graph()

    # Only add nodes for authors who appear in the filtered dataset
    for author_id in authors_in_filtered_set:
        # Get the primary subfield from the previously calculated counts
        primary_subfield = determine_primary_subfield(author_subfield_counts[author_id])
        # Get the country from the previously calculated info
        country = author_countries.get(author_id, "Unknown")
        # Get publication count (from the complete dataset)
        pub_count = author_publication_counts.get(author_id, 0)

        # label1: country, label2: primary subfield, label3: publication count
        G.add_node(author_id, label1=country, label2=primary_subfield, label3=pub_count)

    # Add weighted edges for collaborations
    for (id1, id2), weight in collaboration_edges.items():
        G.add_edge(id1, id2, weight=weight)

    return G


def filter_subfield_publications(df: pd.DataFrame, subfield: str) -> pd.DataFrame:
    """
    Filters the DataFrame to include only publications where the
    subfield's display_name matches the provided subfield.

    The function assumes that the 'subfield' column contains a JSON string.
    """
    # Use a lambda to parse the JSON and check the display_name
    filtered_df = df[
        df["subfield"].apply(lambda x: json.loads(x).get("display_name") == subfield)
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


def filter_publications_by_year(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    Filters the DataFrame to include only publications for a specific year.
    """
    filtered_df = df[df["publication_year"] == year]
    return filtered_df


def filter_publications_with_doi(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters the DataFrame to include only publications with a DOI.
    Returns the filtered DataFrame and the count of publications without DOI.
    """
    # Count publications without DOI
    missing_doi_count = df["doi"].isna().sum()

    # Filter for publications with DOI
    filtered_df = df[df["doi"].notna()]

    return filtered_df, missing_doi_count


def check_duplicate_dois(df: pd.DataFrame) -> tuple:
    """
    Checks for duplicate DOIs in the DataFrame.
    Returns a tuple containing:
    1. The DataFrame with duplicate DOIs removed (keeping the first occurrence)
    2. The count of duplicate DOIs
    3. A DataFrame containing only the duplicate DOIs for inspection
    """
    # Count duplicates
    duplicate_count = df.duplicated(subset=["doi"]).sum()

    # Get duplicate DOIs for inspection
    duplicate_dois = df[df.duplicated(subset=["doi"], keep=False)].sort_values("doi")

    # Remove duplicates (keep first occurrence)
    df_no_duplicates = df.drop_duplicates(subset=["doi"], keep="first")

    return df_no_duplicates, duplicate_count, duplicate_dois


def filter_nodes_by_publication_count(G, min_publications):
    """
    Filters out nodes (authors) from the graph that have fewer than
    the specified minimum number of publications.

    Parameters:
        G (nx.Graph): The collaboration network graph
        min_publications (int): Minimum number of publications required

    Returns:
        nx.Graph: A new graph with filtered nodes
    """
    # Create a copy of the graph to avoid modifying the original
    filtered_G = G.copy()

    # Get nodes to remove (authors with fewer than min_publications)
    nodes_to_remove = [
        node
        for node, attrs in G.nodes(data=True)
        if attrs.get("label3", 0) < min_publications
    ]

    # Remove the identified nodes
    filtered_G.remove_nodes_from(nodes_to_remove)

    print(
        f"Removed {len(nodes_to_remove)} nodes with fewer than {min_publications} publications"
    )
    return filtered_G


def filter_edges_by_weight(G, min_weight):
    """
    Filters out edges from the graph that have a weight less than
    the specified minimum weight. Weight represents the number of collaborations.

    Parameters:
        G (nx.Graph): The collaboration network graph
        min_weight (int): Minimum edge weight required

    Returns:
        nx.Graph: A new graph with filtered edges
    """
    # Create a copy of the graph to avoid modifying the original
    filtered_G = G.copy()

    # Get edges to remove (collaborations with fewer than min_weight)
    edges_to_remove = [
        (u, v)
        for u, v, attrs in G.edges(data=True)
        if attrs.get("weight", 0) < min_weight
    ]

    # Remove the identified edges
    filtered_G.remove_edges_from(edges_to_remove)

    print(f"Removed {len(edges_to_remove)} edges with weight less than {min_weight}")
    return filtered_G


def remove_isolated_nodes(G):
    """
    Removes nodes with no edges (isolated nodes with degree = 0).

    Parameters:
        G (nx.Graph): The collaboration network graph

    Returns:
        nx.Graph: A new graph with isolated nodes removed
    """
    # Create a copy of the graph to avoid modifying the original
    filtered_G = G.copy()

    # Find nodes with degree 0 (no edges)
    isolated_nodes = [node for node, degree in dict(G.degree()).items() if degree == 0]

    # Remove the isolated nodes
    filtered_G.remove_nodes_from(isolated_nodes)

    print(f"Removed {len(isolated_nodes)} isolated nodes with no connections")
    return filtered_G


def main():
    """
    Main function that loads the publications data, builds the collaboration network,
    filters the network based on publication count and collaboration strength,
    removes isolated nodes, and saves the network as a GEXF file.
    """
    # Load the CSV file into a pandas DataFrame
    try:
        full_df = pd.read_csv(INPUT_PATH)
        print(f"Loaded {len(full_df)} publications from CSV file")
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    # Filter publications with DOI
    df_with_doi, missing_doi_count = filter_publications_with_doi(full_df)
    print(f"Number of publications without DOI: {missing_doi_count}")

    # Check for duplicate DOIs
    df_no_duplicates, duplicate_count, duplicate_dois = check_duplicate_dois(
        df_with_doi
    )
    print(f"Number of duplicate DOIs: {duplicate_count}")

    # if duplicate_count > 0:
    #     print("First few duplicate DOIs for inspection:")
    #     print(duplicate_dois[["doi", "title"]].head())

    # Calculate author subfield counts using the complete dataset (with only DOI filtering)
    print("Calculating author primary subfields from complete dataset...")
    author_subfield_counts, author_countries, author_publication_counts = (
        calculate_author_subfield_counts(df_no_duplicates)
    )
    print(f"Calculated primary subfields for {len(author_subfield_counts)} authors")

    # Filter by citation count for the collaboration network
    df_citations_filtered = filter_publications_by_citation_count(
        df_no_duplicates, MIN_CITATIONS
    )
    print(
        f"Publications with > {MIN_CITATIONS} citations: {len(df_citations_filtered)}"
    )

    # Build the collaboration network graph using citation-filtered data but with subfields from complete data
    G = build_collaboration_network(
        df_citations_filtered,
        author_subfield_counts,
        author_countries,
        author_publication_counts,
    )
    print(
        f"Built collaboration network with {len(G.nodes())} nodes and {len(G.edges())} edges"
    )

    # Remove collaborations with fewer than N joint papers
    G_filtered_edges = filter_edges_by_weight(G, MIN_WEIGHT)

    # Remove isolated nodes (nodes with no connections)
    G_filtered = remove_isolated_nodes(G_filtered_edges)

    print(
        f"Final filtered network has {len(G_filtered.nodes())} nodes and {len(G_filtered.edges())} edges"
    )

    # Write the fully filtered graph to a GEXF file
    output_file = f"{OUTPUT_PATH}/filters/collabnet_filtered.gexf"

    try:
        nx.write_gexf(G_filtered, output_file)
        print(f"Filtered graph successfully written to {output_file}")
    except Exception as e:
        print(f"Error writing GEXF file: {e}")


if __name__ == "__main__":
    main()
