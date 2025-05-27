import json
import pandas as pd
import networkx as nx
from itertools import combinations
from collections import defaultdict
import os
from typing import List, Dict, Tuple, Any, Optional, Set

# --- Constants ---
INPUT_PATH = "../../data/raw/publication_meta/br_publication_meta.csv"
OUTPUT_PATH = "../../data/graphs"
FULL_GRAPH_DIR_NAME = "full"
FULL_GRAPH_FILENAME = "_collabnet_full.gexf"
RECURRENT_H_CITED_GRAPH = "_recurrent_highly_cited_net.gexf"
HIGHLY_CITED_GRAPH = "_highly_cited_net.gexf"
PUBLICATION_COUNTS_FILENAME_SUFFIX = "_publication_counts.json"

# Filters for the "filtered" network
MIN_CITATIONS_FOR_FILTERED_GRAPH = 40
MIN_EDGE_WEIGHT_FOR_FILTERED_GRAPH = 2

# List of subfields to generate networks for (based on user's example output)
SUBFIELDS = [
    "Artificial Intelligence",
    "Computational Theory and Mathematics",
    "Computer Graphics and Computer-Aided Design",
    "Computer Networks and Communications",
    "Computer Science Applications",
    "Computer Vision and Pattern Recognition",
    "Hardware and Architecture",
    "Human-Computer Interaction",
    "Information Systems",
    "Signal Processing",
    "Software"
]

# --- Utility Functions ---

def _ensure_dir_exists(path: str) -> None:
    """Ensures that the directory at the given path exists."""
    os.makedirs(path, exist_ok=True)
    print(f"Ensured directory exists: {path}")

def _save_graph_to_gexf(graph: nx.Graph, file_path: str) -> None:
    """Saves a NetworkX graph to a GEXF file."""
    try:
        nx.write_gexf(graph, file_path)
        print(f"Graph successfully written to {file_path}")
    except Exception as e:
        print(f"Error writing GEXF file {file_path}: {e}")

def _save_publication_count(count: int, file_path: str) -> None:
    """Saves the publication count to a JSON file."""
    try:
        with open(file_path, 'w') as f:
            json.dump({"publication_count": count}, f)
        print(f"Publication count successfully written to {file_path}")
    except Exception as e:
        print(f"Error writing publication count file {file_path}: {e}")

# --- JSON Parsing and Author Data Extraction ---

def parse_json_field(field_str: Optional[str]) -> Optional[Any]:
    """
    Safely parse a JSON-formatted string.
    Returns a Python object (list or dict) if parsing is successful and input is not None;
    otherwise returns None.
    """
    if pd.isna(field_str) or not isinstance(field_str, str):
        return None
    try:
        return json.loads(field_str)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e} for input: {field_str[:100]}...")
        return None

def get_author_country(author: Dict[str, Any]) -> str:
    """
    Extracts the country of the author from their institution data.
    Returns the first country code if available, else 'Unknown'.
    """
    countries: List[str] = author.get("countries", [])
    if countries:
        return countries[0]
    return "Unknown"

def determine_primary_subfield(subfield_count: Dict[str, int]) -> str:
    """
    Given a dictionary of subfield counts for an author,
    returns the subfield with the highest count.
    If there is a tie, one is selected arbitrarily by max().
    If the dictionary is empty, returns 'Unknown'.
    """
    if not subfield_count:
        return "Unknown"
    return max(subfield_count.items(), key=lambda x: x[1])[0]

# --- Data Loading and Cleaning ---

def deduplicate_publications(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply a comprehensive deduplication strategy matching the one in 2_2_concat_publication_meta.py.
    
    Strategy:
    1. First remove exact duplicates (all columns identical)
    2. Handle ID and DOI duplicates with a carefully designed priority system
    3. Prioritize keeping the most complete/recent records when duplicates are found
    
    Parameters:
    df (pandas.DataFrame): DataFrame to be deduplicated
    
    Returns:
    pandas.DataFrame: Deduplicated DataFrame
    """
    initial_count = len(df)
    print(f"Initial dataset has {initial_count} rows")
    
    # Step 1: Remove exact duplicates (all columns identical)
    print("\nRemoving exact duplicate rows (all columns identical)...")
    df.drop_duplicates(inplace=True)
    after_exact_dedup_count = len(df)
    print(f"After removing exact duplicates: {after_exact_dedup_count} rows")
    print(f"Removed {initial_count - after_exact_dedup_count} exact duplicate rows")
    
    # Step 2: Handle 'id' and 'doi' duplicates with a priority strategy
    # First, fill NaN DOIs with empty string to simplify processing
    df['doi'].fillna('', inplace=True)
    
    # Identify rows with duplicate IDs or DOIs
    duplicate_ids = df.duplicated(subset=['id'], keep=False)
    duplicate_dois = df[df['doi'] != ''].duplicated(subset=['doi'], keep=False)
    
    print(f"\nFound {duplicate_ids.sum()} rows with duplicate 'id' values")
    print(f"Found {duplicate_dois.sum()} rows with duplicate 'doi' values (excluding empty DOIs)")
    
    # Create a helper column for sorting and prioritizing which duplicates to keep
    df['dedup_priority'] = 0
    
    # Strategy: Prioritize rows based on:
    # 1. Having a non-empty DOI
    # 2. Having more citation data (cited_by_count)
    # 3. More recent data (higher publication_year)
    
    # Give priority to records with DOIs
    df.loc[df['doi'] != '', 'dedup_priority'] += 10
    
    # Give priority based on citation data presence
    df['dedup_priority'] += df['cited_by_count'].notna().astype(int) * 5
    
    # Give priority based on citation count (normalize to 0-3 range)
    max_cite = df['cited_by_count'].max() if df['cited_by_count'].max() > 0 else 1
    df['dedup_priority'] += (df['cited_by_count'].fillna(0) / max_cite * 3)
    
    # Give slight priority to more recent publications
    # Normalize publication year to 0-1 range within our dataset
    years = pd.to_numeric(df['publication_year'], errors='coerce').fillna(0)
    min_year = years.min() if years.min() > 0 else 2015
    max_year = years.max() if years.max() > 0 else 2024
    year_range = max_year - min_year if max_year > min_year else 1
    df['dedup_priority'] += ((years - min_year) / year_range)
    
    # Sort by dedup_priority (descending) to keep the best record first
    df.sort_values('dedup_priority', ascending=False, inplace=True)
    
    # Now perform the deduplication, keeping the first occurrence (highest priority)
    print("\nRemoving duplicate 'id' values, keeping the highest quality record...")
    df_dedup_id = df.drop_duplicates(subset=['id'], keep='first')
    after_id_dedup_count = len(df_dedup_id)
    print(f"After 'id' deduplication: {after_id_dedup_count} rows")
    print(f"Removed {after_exact_dedup_count - after_id_dedup_count} rows with duplicate 'id' values")
    
    # Now deduplicate based on DOI, but only for non-empty DOIs
    print("\nRemoving duplicate 'doi' values (where DOI is not empty)...")
    # Create a mask for rows with non-empty DOIs
    non_empty_doi_mask = df_dedup_id['doi'] != ''
    
    # Split into dataframes with and without DOIs
    df_with_doi = df_dedup_id[non_empty_doi_mask]
    df_without_doi = df_dedup_id[~non_empty_doi_mask]
    
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

def load_and_clean_publications(file_path: str) -> Optional[pd.DataFrame]:
    """
    Loads publications from a CSV file and applies the same cleaning criteria
    as in the original 2_2_concat_publication_meta.py script.
    
    Returns a cleaned DataFrame or None if loading fails.
    """
    print(f"Loading publications from: {file_path}")
    try:
        df = pd.read_csv(file_path)
        print(f"Successfully loaded {len(df)} raw publications.")
    except FileNotFoundError:
        print(f"Error: Input file not found at {file_path}")
        return None
    except Exception as e:
        print(f"Error reading CSV file {file_path}: {e}")
        return None

    # Apply comprehensive deduplication strategy (matching 2_2_concat_publication_meta.py)
    df_cleaned = deduplicate_publications(df)
    
    print(f"Data after cleaning and deduplication: {len(df_cleaned)} publications.")
    return df_cleaned

# --- Author Metadata Calculation ---

AuthorSubfieldCounts = Dict[str, Dict[str, int]]
AuthorInfo = Dict[str, str] # Author ID -> Country
AuthorPublicationCounts = Dict[str, int]

def extract_author_metadata(df: pd.DataFrame) -> Tuple[AuthorSubfieldCounts, AuthorInfo, AuthorPublicationCounts]:
    """
    Processes the publications DataFrame to calculate subfield counts, country,
    and total publication counts for each author.

    Args:
        df: DataFrame containing publication data with 'authorships' and 'subfield' columns.

    Returns:
        A tuple containing:
        - author_subfield_counts: Dictionary mapping author IDs to their subfield counts.
        - author_info: Dictionary mapping author IDs to their primary country.
        - author_publication_counts: Dictionary mapping author IDs to their total publication counts.
    """
    print("Calculating author metadata (subfields, countries, publication counts)...")
    author_info: AuthorInfo = {}
    author_subfield_counts: AuthorSubfieldCounts = defaultdict(lambda: defaultdict(int))
    author_publication_counts: AuthorPublicationCounts = defaultdict(int)

    for _, row in df.iterrows():
        authors = parse_json_field(row.get("authorships"))
        if not isinstance(authors, list):
            continue

        subfield_data = parse_json_field(row.get("subfield"))
        subfield_name = subfield_data.get("display_name", "Unknown") if isinstance(subfield_data, dict) else "Unknown"

        for author_data in authors:
            if not isinstance(author_data, dict):
                continue
            author_id = author_data.get("id")
            if not author_id:
                continue
            
            author_id = str(author_id) # Ensure author_id is a string

            if author_id not in author_info:
                author_info[author_id] = get_author_country(author_data)
            
            author_subfield_counts[author_id][subfield_name] += 1
            author_publication_counts[author_id] += 1
            
    print(f"Extracted metadata for {len(author_info)} unique authors.")
    return author_subfield_counts, author_info, author_publication_counts

# --- Network Building ---

def build_collaboration_network(
    publications_df: pd.DataFrame,
    author_subfield_counts: AuthorSubfieldCounts,
    author_countries: AuthorInfo,
    author_publication_counts: AuthorPublicationCounts,
) -> nx.Graph:
    """
    Builds a collaboration network from publications data.

    Nodes represent authors, and edges represent collaborations.
    Node attributes include country, primary subfield, and total publication count.
    Edge attributes include weight (number of joint publications).

    Args:
        publications_df: DataFrame of publications to consider for collaborations.
        author_subfield_counts: Pre-calculated subfield counts for all authors.
        author_countries: Pre-calculated country for all authors.
        author_publication_counts: Pre-calculated publication counts for all authors.

    Returns:
        A NetworkX graph representing the collaboration network.
    """
    print(f"Building collaboration network from {len(publications_df)} publications...")
    collaboration_edges: Dict[Tuple[str, str], int] = defaultdict(int)
    authors_in_this_network: Set[str] = set()

    for _, row in publications_df.iterrows():
        authors_data = parse_json_field(row.get("authorships"))
        if not isinstance(authors_data, list):
            continue

        current_publication_author_ids: List[str] = []
        for author in authors_data:
            if isinstance(author, dict) and author.get("id"):
                author_id = str(author.get("id"))
                current_publication_author_ids.append(author_id)
                authors_in_this_network.add(author_id)
        
        # Create edges for collaborations on this publication
        for id1, id2 in combinations(sorted(current_publication_author_ids), 2):
            # Ensure consistent edge representation (sorted tuple)
            edge = tuple(sorted((id1, id2)))
            collaboration_edges[edge] += 1

    G = nx.Graph()
    for author_id in authors_in_this_network:
        primary_subfield = determine_primary_subfield(author_subfield_counts.get(author_id, {}))
        country = author_countries.get(author_id, "Unknown")
        pub_count = author_publication_counts.get(author_id, 0)
        
        G.add_node(
            author_id,
            label1=country, # Country
            label2=primary_subfield, # Primary Subfield
            label3=pub_count # Total Publication Count
        )

    for (id1, id2), weight in collaboration_edges.items():
        # Ensure nodes exist before adding edge (should be guaranteed by authors_in_this_network logic)
        if G.has_node(id1) and G.has_node(id2):
            G.add_edge(id1, id2, weight=weight)
        else:
            print(f"Warning: Skipping edge ({id1}, {id2}) as one or both nodes not in graph. This shouldn't happen.")
            
    print(f"Built network with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges.")
    return G

# --- Publication and Graph Filtering Functions ---

def filter_publications_by_citation_count(df: pd.DataFrame, min_citations: int) -> pd.DataFrame:
    """Filters DataFrame for publications with at least 'min_citations'."""
    print(f"Filtering publications by citation count (>={min_citations})...")
    filtered_df = df[df["cited_by_count"] >= min_citations].copy()
    print(f"{len(filtered_df)} publications after citation filtering.")
    return filtered_df

def filter_edges_by_weight(graph: nx.Graph, min_weight: int) -> nx.Graph:
    """Filters edges in a graph by minimum weight."""
    print(f"Filtering edges by minimum weight ({min_weight})...")
    G_filtered = graph.copy()
    edges_to_remove = [
        (u, v) for u, v, attrs in G_filtered.edges(data=True) if attrs.get("weight", 0) < min_weight
    ]
    G_filtered.remove_edges_from(edges_to_remove)
    print(f"Removed {len(edges_to_remove)} edges with weight less than {min_weight}.")
    return G_filtered

def remove_isolated_nodes(graph: nx.Graph) -> nx.Graph:
    """Removes nodes with no edges (degree 0)."""
    print("Removing isolated nodes...")
    G_filtered = graph.copy()
    isolated_nodes = [node for node, degree in dict(G_filtered.degree()).items() if degree == 0]
    G_filtered.remove_nodes_from(isolated_nodes)
    print(f"Removed {len(isolated_nodes)} isolated nodes.")
    return G_filtered

# Optional filter: Not used in main flow by default, but available.
def filter_nodes_by_publication_count(graph: nx.Graph, min_publications: int) -> nx.Graph:
    """
    Filters out nodes (authors) from the graph that have fewer than
    the specified minimum number of publications (based on 'label3' attribute).
    """
    print(f"Filtering nodes by minimum publication count ({min_publications})...")
    G_filtered = graph.copy()
    nodes_to_remove = [
        node for node, attrs in G_filtered.nodes(data=True) if attrs.get("label3", 0) < min_publications
    ]
    G_filtered.remove_nodes_from(nodes_to_remove)
    print(f"Removed {len(nodes_to_remove)} nodes with fewer than {min_publications} total publications.")
    return G_filtered

# --- Main Execution ---

def generate_and_save_full_network(
    cleaned_df: pd.DataFrame,
    author_metadata: Tuple[AuthorSubfieldCounts, AuthorInfo, AuthorPublicationCounts],
    output_directory: str,
    filename: str
) -> None:
    """Generates and saves the full collaboration network."""
    print("\n--- Generating Full Collaboration Network ---")
    author_subfield_counts, author_countries, author_publication_counts = author_metadata
    
    G_full = build_collaboration_network(
        cleaned_df,
        author_subfield_counts,
        author_countries,
        author_publication_counts,
    )
    
    output_file_path = os.path.join(output_directory, filename)
    _save_graph_to_gexf(G_full, output_file_path)

    # Save publication count for the full network
    pub_count_file_path = os.path.join(output_directory, filename.replace(".gexf", PUBLICATION_COUNTS_FILENAME_SUFFIX))
    _save_publication_count(len(cleaned_df), pub_count_file_path)

    print(f"Full network processing complete. Nodes: {G_full.number_of_nodes()}, Edges: {G_full.number_of_edges()}")

def generate_and_save_filtered_network(
    cleaned_df: pd.DataFrame,
    author_metadata: Tuple[AuthorSubfieldCounts, AuthorInfo, AuthorPublicationCounts],
    output_directory: str,
    filename: str,
    min_citations: int,
    min_edge_weight: int
) -> None:
    """Generates and saves the filtered collaboration network."""
    print("\n--- Generating Filtered Collaboration Network ---")
    author_subfield_counts, author_countries, author_publication_counts = author_metadata

    # 1. Filter publications by citation count
    df_citations_filtered = filter_publications_by_citation_count(cleaned_df, min_citations)
    
    if df_citations_filtered.empty:
        print("No publications remained after citation filtering. Filtered network will be empty.")
        G_final_filtered = nx.Graph()
        pub_count_for_filtered_network = 0
    else:
        # 2. Build initial graph from citation-filtered data
        G_base_filtered = build_collaboration_network(
            df_citations_filtered,
            author_subfield_counts,
            author_countries,
            author_publication_counts,
        )
        
        # 3. Filter edges by weight
        G_edges_filtered = filter_edges_by_weight(G_base_filtered, min_edge_weight)
        
        # 4. Remove isolated nodes
        G_final_filtered = remove_isolated_nodes(G_edges_filtered)
        
        pub_count_for_filtered_network = len(df_citations_filtered)


    output_file_path = os.path.join(output_directory, filename)
    _save_graph_to_gexf(G_final_filtered, output_file_path)

    # Save publication count for the filtered network
    pub_count_file_path = os.path.join(output_directory, filename.replace(".gexf", PUBLICATION_COUNTS_FILENAME_SUFFIX))
    _save_publication_count(pub_count_for_filtered_network, pub_count_file_path)

    print(f"Filtered network processing complete. Nodes: {G_final_filtered.number_of_nodes()}, Edges: {G_final_filtered.number_of_edges()}")

def generate_and_save_subfield_networks(
    cleaned_df: pd.DataFrame,
    author_metadata: Tuple[AuthorSubfieldCounts, AuthorInfo, AuthorPublicationCounts],
    output_directory: str
) -> None:
    """Generates and saves collaboration networks for each subfield."""
    print("\n--- Generating Subfield Collaboration Networks ---")
    author_subfield_counts, author_countries, author_publication_counts = author_metadata

    for subfield in SUBFIELDS:
        sanitized_subfield = subfield.replace(" ", "_")
        print(f"Processing subfield: {subfield}")

        # Filter publications for the current subfield
        # Ensure 'subfield' column is parsed correctly before filtering
        subfield_df = cleaned_df[
            cleaned_df["subfield"].apply(
                lambda x: parse_json_field(x).get("display_name") == subfield if parse_json_field(x) else False
            )
        ].copy() # Use .copy() to avoid SettingWithCopyWarning

        if subfield_df.empty:
            print(f"No publications found for subfield '{subfield}'. Skipping network generation.")
            continue

        # Build collaboration network for the subfield
        G_subfield = build_collaboration_network(
            subfield_df,
            author_subfield_counts,
            author_countries,
            author_publication_counts,
        )

        # Remove isolated nodes (authors who only published in this subfield but have no co-authors)
        G_subfield = remove_isolated_nodes(G_subfield)

        # Define output file paths
        output_file_name = f"{sanitized_subfield}.gexf"
        output_file_path = os.path.join(output_directory, output_file_name)
        pub_count_file_path = os.path.join(output_directory, output_file_name.replace(".gexf", PUBLICATION_COUNTS_FILENAME_SUFFIX))

        # Save graph and publication count
        _save_graph_to_gexf(G_subfield, output_file_path)
        _save_publication_count(len(subfield_df), pub_count_file_path)

        print(f"Subfield network '{subfield}' processing complete. Nodes: {G_subfield.number_of_nodes()}, Edges: {G_subfield.number_of_edges()}")


def main() -> None:
    """
    Main function to load data, process, and generate collaboration networks.
    """
    print("Starting collaboration network generation process...")

    # --- Setup Output Directories ---
    full_graph_output_path = os.path.join(OUTPUT_PATH, FULL_GRAPH_DIR_NAME)
    filtered_graph_output_path = os.path.join(OUTPUT_PATH, FULL_GRAPH_DIR_NAME) # Using the same 'full' directory for filtered and subfield graphs
    _ensure_dir_exists(full_graph_output_path)
    _ensure_dir_exists(filtered_graph_output_path)

    # --- Load and Clean Data ---
    # Now applying the same deduplication strategy as the first script
    cleaned_publications_df = load_and_clean_publications(INPUT_PATH)
    if cleaned_publications_df is None or cleaned_publications_df.empty:
        print("Failed to load or clean data, or data is empty. Exiting.")
        return

    # --- Extract Author Metadata (done once on fully cleaned data) ---
    # This metadata is used for enriching nodes in ALL networks.
    author_metadata_tuple = extract_author_metadata(cleaned_publications_df)
    if not author_metadata_tuple[1]: # Check if author_info (countries) is empty
        print("No author metadata could be extracted. Check data and parsing. Exiting.")
        return

    # --- Generate and Save Full Network ---
    generate_and_save_full_network(
        cleaned_publications_df,
        author_metadata_tuple,
        full_graph_output_path,
        FULL_GRAPH_FILENAME
    )

    # --- Generate and Save Highly Cited Network ---
    generate_and_save_filtered_network(
        cleaned_publications_df,
        author_metadata_tuple,
        filtered_graph_output_path,
        HIGHLY_CITED_GRAPH,
        min_edge_weight=1,
        min_citations=MIN_CITATIONS_FOR_FILTERED_GRAPH,
    )

    # --- Generate and Save Recurrent Highly Cited Network ---
    generate_and_save_filtered_network(
        cleaned_publications_df,
        author_metadata_tuple,
        filtered_graph_output_path,
        RECURRENT_H_CITED_GRAPH,
        min_citations=MIN_CITATIONS_FOR_FILTERED_GRAPH,
        min_edge_weight=MIN_EDGE_WEIGHT_FOR_FILTERED_GRAPH
    )

    # --- Generate and Save Subfield Networks ---
    generate_and_save_subfield_networks(
        cleaned_df=cleaned_publications_df,
        author_metadata=author_metadata_tuple,
        output_directory=full_graph_output_path # Save subfield networks in the 'full' directory
    )

    print("\nCollaboration network generation process finished.")


if __name__ == "__main__":
    main()