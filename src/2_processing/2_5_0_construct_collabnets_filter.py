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

def filter_publications_with_doi(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    """
    Filters the DataFrame to include only publications with a DOI.
    Returns the filtered DataFrame and the count of publications without DOI.
    """
    missing_doi_count = df["doi"].isna().sum()
    filtered_df = df[df["doi"].notna()].copy() # Use .copy() to avoid SettingWithCopyWarning
    print(f"Identified {missing_doi_count} publications without DOI.")
    return filtered_df, missing_doi_count

def check_and_remove_duplicate_dois(df: pd.DataFrame) -> Tuple[pd.DataFrame, int, pd.DataFrame]:
    """
    Checks for duplicate DOIs in the DataFrame.
    Returns a tuple containing:
    1. The DataFrame with duplicate DOIs removed (keeping the first occurrence).
    2. The count of duplicate DOIs removed.
    3. A DataFrame containing only the duplicate DOIs (all occurrences) for inspection.
    """
    duplicate_mask = df.duplicated(subset=["doi"], keep=False)
    duplicate_dois_df = df[duplicate_mask].sort_values("doi")
    
    num_duplicates_to_remove = df.duplicated(subset=["doi"], keep='first').sum()
    
    df_no_duplicates = df.drop_duplicates(subset=["doi"], keep="first").copy() # Use .copy()
    print(f"Removed {num_duplicates_to_remove} duplicate DOI entries.")
    return df_no_duplicates, num_duplicates_to_remove, duplicate_dois_df

def load_and_clean_publications(file_path: str) -> Optional[pd.DataFrame]:
    """
    Loads publications from a CSV file, filters for valid DOIs, and removes duplicates.
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

    df_with_doi, _ = filter_publications_with_doi(df)
    df_cleaned, _, _ = check_and_remove_duplicate_dois(df_with_doi)
    
    print(f"Data after DOI cleaning and deduplication: {len(df_cleaned)} publications.")
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
    """Filters DataFrame for publications with more than 'min_citations'."""
    print(f"Filtering publications by citation count (>{min_citations})...")
    filtered_df = df[df["cited_by_count"] > min_citations].copy()
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
    cleaned_publications_df = load_and_clean_publications(INPUT_PATH)
    if cleaned_publications_df is None or cleaned_publications_df.empty:
        print("Failed to load or clean data, or data is empty. Exiting.")
        return

    # --- Extract Author Metadata (done once on fully cleaned data) ---
    # This metadata is used for enriching nodes in BOTH networks.
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
        min_citations=MIN_CITATIONS_FOR_FILTERED_GRAPH,
        min_edge_weight=MIN_EDGE_WEIGHT_FOR_FILTERED_GRAPH
    )

    # --- Generate and Save Recurrent Highly Citated Network ---
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
        cleaned_publications_df,
        author_metadata_tuple,
        full_graph_output_path # Save subfield networks in the 'full' directory
    )

    print("\nCollaboration network generation process finished.")


if __name__ == "__main__":
    main()