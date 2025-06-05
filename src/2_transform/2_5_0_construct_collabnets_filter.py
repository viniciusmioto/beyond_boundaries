import json
import pandas as pd
import networkx as nx
from itertools import combinations
from collections import defaultdict
import os
from typing import List, Dict, Tuple, Any, Optional, Set

# --- Constants ---
INPUT_PATH = "../../data/raw/publication_meta/br_publication_meta.csv"
OUTPUT_PATH = "../../data/graphs/"
FULL_GRAPH_DIR_NAME = "full"
FULL_GRAPH_FILENAME = "_collabnet_full.gexf"
HIGHLY_CITED_GRAPH = "_highly_cited_net.gexf"
RECURRENT_H_CITED_GRAPH = "_recurrent_highly_cited_net.gexf"
PUBLICATION_COUNTS_FILENAME_SUFFIX = "_publication_counts.json"
MIN_CITATIONS_FOR_HC = 40
MIN_EDGE_WEIGHT_FOR_RHC = 2


# List of subfields to generate networks for
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

# --- Data Loading ---

def load_publications(file_path: str) -> Optional[pd.DataFrame]:
    """
    Loads pre-cleaned publications from a CSV file.
    Returns the DataFrame or None if loading fails.
    """
    print(f"Loading publications from: {file_path}")
    try:
        df = pd.read_csv(file_path)
        print(f"Successfully loaded {len(df)} publications.")
        return df
    except FileNotFoundError:
        print(f"Error: Input file not found at {file_path}")
        return None
    except Exception as e:
        print(f"Error reading CSV file {file_path}: {e}")
        return None

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

# --- Network Building (Core Function) ---

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
        # Ensure nodes exist before adding edge
        if G.has_node(id1) and G.has_node(id2):
            G.add_edge(id1, id2, weight=weight)
        else:
            print(f"Warning: Skipping edge ({id1}, {id2}) as one or both nodes not in graph.")
            
    print(f"Built network with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges.")
    return G

# --- Network Generation and Saving ---

def generate_and_save_network(
    df: pd.DataFrame,
    author_metadata: Tuple[AuthorSubfieldCounts, AuthorInfo, AuthorPublicationCounts],
    output_directory: str,
    filename: str
) -> None:
    """Generates and saves a collaboration network."""
    print(f"\n--- Generating Network: {filename} ---")
    author_subfield_counts, author_countries, author_publication_counts = author_metadata
    
    G = build_collaboration_network(
        df,
        author_subfield_counts,
        author_countries,
        author_publication_counts,
    )
    
    output_file_path = os.path.join(output_directory, filename)
    _save_graph_to_gexf(G, output_file_path)

    # Save publication count 
    pub_count_file_path = os.path.join(output_directory, filename.replace(".gexf", PUBLICATION_COUNTS_FILENAME_SUFFIX))
    _save_publication_count(len(df), pub_count_file_path)

    print(f"Network processing complete. Nodes: {G.number_of_nodes()}, Edges: {G.number_of_edges()}")

def generate_and_save_subfield_networks(
    df: pd.DataFrame,
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
        subfield_df = df[
            df["subfield"].apply(
                lambda x: parse_json_field(x).get("display_name") == subfield if parse_json_field(x) else False
            )
        ].copy()

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

        # Define output file paths
        output_file_name = f"{sanitized_subfield}.gexf"
        output_file_path = os.path.join(output_directory, output_file_name)
        pub_count_file_path = os.path.join(output_directory, output_file_name.replace(".gexf", PUBLICATION_COUNTS_FILENAME_SUFFIX))

        # Save graph and publication count
        _save_graph_to_gexf(G_subfield, output_file_path)
        _save_publication_count(len(subfield_df), pub_count_file_path)

        print(f"Subfield network '{subfield}' processing complete. Nodes: {G_subfield.number_of_nodes()}, Edges: {G_subfield.number_of_edges()}")

# --- Main Execution ---

def main() -> None:
    """
    Main function to load data and generate collaboration networks.
    Assumes that input data has already been cleaned and filtered.
    """
    print("Starting collaboration network generation process...")

    # --- Setup Output Directories ---
    output_path = os.path.join(OUTPUT_PATH, FULL_GRAPH_DIR_NAME)
    _ensure_dir_exists(output_path)

    # --- Load Data (assumes it's already cleaned) ---
    publications_df = load_publications(INPUT_PATH)
    if publications_df is None or publications_df.empty:
        print("Failed to load data, or data is empty. Exiting.")
        return

    # --- Extract Author Metadata (done once for all networks) ---
    author_metadata_tuple = extract_author_metadata(publications_df)
    if not author_metadata_tuple[1]: # Check if author_info (countries) is empty
        print("No author metadata could be extracted. Check data and parsing. Exiting.")
        return

    # --- Generate and Save Full Network ---
    # generate_and_save_network(
    #     publications_df,
    #     author_metadata_tuple,
    #     output_path,
    #     FULL_GRAPH_FILENAME
    # )
    
    # # --- Generate and Save Subfield Networks ---
    # # This uses the same author metadata but filters publications by subfield
    # generate_and_save_subfield_networks(
    #     publications_df,
    #     author_metadata_tuple,
    #     output_path
    # )

    print("\nCollaboration network generation process finished.")

    # --- Generate and Save Highly Cited Network (>= 40 citations) ---
    highly_cited_df = publications_df[publications_df["cited_by_count"] >= MIN_CITATIONS_FOR_HC]
    if not highly_cited_df.empty:
        generate_and_save_network(
            highly_cited_df,
            author_metadata_tuple,
            output_path,
            HIGHLY_CITED_GRAPH
        )
    else:
        print("No publications with 40 or more citations found for highly cited network.")

    # --- Generate and Save Recurrent Highly Cited Network (>= 40 citations, >= 2 edge weight) ---
    # load the highly cited network to filter recurrent authors
    highly_cited_network_path = os.path.join(output_path, HIGHLY_CITED_GRAPH)
    if os.path.exists(highly_cited_network_path):
        highly_cited_network = nx.read_gexf(highly_cited_network_path)
        
        # Remove edges with weight < 2
        recurrent_edges = [(u, v) for u, v, d in highly_cited_network.edges(data=True) if d['weight'] >= MIN_EDGE_WEIGHT_FOR_RHC]
        recurrent_hc_network = highly_cited_network.edge_subgraph(recurrent_edges).copy()
        recurrent_hc_network.remove_nodes_from(list(nx.isolates(recurrent_hc_network)))

        if recurrent_hc_network.number_of_nodes() > 0:
            recurrent_hc_output_path = os.path.join(output_path, RECURRENT_H_CITED_GRAPH)
            _save_graph_to_gexf(recurrent_hc_network, recurrent_hc_output_path)

            # Save publication count for recurrent highly cited network
            pub_count_file_path = os.path.join(output_path, RECURRENT_H_CITED_GRAPH.replace(".gexf", PUBLICATION_COUNTS_FILENAME_SUFFIX))
            _save_publication_count(0, pub_count_file_path)
            print(f"Recurrent highly cited network saved with {recurrent_hc_network.number_of_nodes()} nodes and {recurrent_hc_network.number_of_edges()} edges.")
        else:
            print("No recurrent highly cited authors found with sufficient collaboration.")
    else:
        print(f"Highly cited network file not found at {highly_cited_network_path}. Cannot generate recurrent highly cited network.")


if __name__ == "__main__":
    main()