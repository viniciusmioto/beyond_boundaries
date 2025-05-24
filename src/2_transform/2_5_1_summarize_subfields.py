# Modified 2_12_summarize_subfields.py
#!/usr/bin/env python3
"""
Graph Metrics Calculator

This script reads all GEXF graph files in a directory, computes basic metrics
(number of nodes and edges, density, fragmentation, distance-weighted fragmentation,
clustering coefficient, and nodes in the largest connected component), and saves the
results to a CSV file.
"""

import os
import csv
import logging
import networkx as nx
from pathlib import Path
from typing import Dict, List, Any
import json # Added import for json

# Constants for input/output
INPUT_DIRECTORY = Path(
    "../../data/graphs/full/"
)  # Path to directory containing GEXF files
OUTPUT_FILE = Path(
    "summary_metrics.csv"
)  # CSV file name to write results in the input directory
PUBLICATION_COUNTS_FILENAME_SUFFIX = "_publication_counts.json" # New constant for publication counts

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

def get_gexf_files(directory: Path) -> List[Path]:
    """
    Returns a list of all GEXF files in the given directory.

    Args:
        directory: Path to the directory to search for GEXF files.

    Returns:
        List of Path objects for each GEXF file.
    """
    resolved_directory = directory.resolve()
    gexf_files = list(resolved_directory.glob("*.gexf"))
    if not gexf_files:
        logger.warning(f"No GEXF files found in {resolved_directory}")
    else:
        logger.info(f"Found {len(gexf_files)} GEXF files in {resolved_directory}")
    return gexf_files

def load_publication_count(file_path: Path) -> int:
    """
    Loads the publication count from a JSON file.
    """
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            return data.get("publication_count", 0)
    except FileNotFoundError:
        logger.warning(f"Publication count file not found: {file_path}")
        return 0
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from {file_path}: {e}")
        return 0
    except Exception as e:
        logger.error(f"Error loading publication count from {file_path}: {e}")
        return 0

def fragmentation(G: nx.Graph) -> float:
    """
    Compute Borgatti's fragmentation F:
    F = 1 - sum_k s_k(s_k-1) / [n(n-1)],
    where s_k are connected component sizes.
    """
    n = G.number_of_nodes()
    if n < 2:
        return 0.0
    sizes = [len(c) for c in nx.connected_components(G)]
    num = sum(s * (s - 1) for s in sizes)
    return 1 - num / (n * (n - 1))


def nodes_in_largest_component(G: nx.Graph) -> int:
    """
    Returns the size (number of nodes) of the largest connected component.
    Uses weakly connected components for directed graphs.
    """
    if G.number_of_nodes() == 0:
        return 0
    if G.is_directed():
        comps = nx.weakly_connected_components(G)
    else:
        comps = nx.connected_components(G)
    return max((len(c) for c in comps), default=0)


def compute_graph_metrics(file_path: Path) -> Dict[str, Any]:
    """
    Computes metrics for a single graph file.

    Args:
        file_path: Path to the GEXF file.

    Returns:
        Dictionary containing graph metrics.
    """
    try:
        logger.info(f"Processing {file_path.name}")
        graph = nx.read_gexf(file_path)

        logger.info("Calculating number of nodes and edges")
        num_nodes = graph.number_of_nodes()
        num_edges = graph.number_of_edges()

        logger.info("Calculating fragmentation")
        frag = fragmentation(graph)

        logger.info("Calculating clustering coefficient")
        clust_coeff = nx.average_clustering(graph)

        logger.info("Calculating nodes in the largest component")
        largest_comp = nodes_in_largest_component(graph)

        # Load publication count
        pub_count_file_path = file_path.parent / (file_path.stem + PUBLICATION_COUNTS_FILENAME_SUFFIX)
        publication_count = load_publication_count(pub_count_file_path)

        return {
            "filename": file_path.stem,
            "nodes": num_nodes,
            "edges": num_edges,
            "fragmentation": frag,
            "clustering_coefficient": clust_coeff,
            "nodes_in_largest_component": largest_comp,
            "publication_count": publication_count # Added publication count
        }
    except Exception as e:
        logger.error(f"Error processing {file_path.name}: {e}")
        return {
            "filename": file_path.stem,
            "nodes": "ERROR",
            "edges": "ERROR",
            "fragmentation": "ERROR",
            "clustering_coefficient": "ERROR",
            "nodes_in_largest_component": "ERROR",
            "publication_count": "ERROR" # Added publication count
        }


def save_to_csv(results: List[Dict[str, Any]], output_file: Path) -> None:
    """
    Saves the list of metric dictionaries to a CSV file.
    """
    if not results:
        logger.warning("No results to save")
        return

    fieldnames = [
        "filename",
        "nodes",
        "edges",
        "fragmentation",
        "clustering_coefficient",
        "nodes_in_largest_component",
        "publication_count" # Added publication_count to fieldnames
    ]

    try:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for result in results:
                writer.writerow(result)
        logger.info(f"Results saved to {output_file}")
    except Exception as e:
        logger.error(f"Failed to save CSV: {e}")


def process_all_graphs(gexf_files: List[Path]) -> List[Dict[str, Any]]:
    """
    Process all graph files and compute metrics one by one.
    """
    results = []
    for file_path in gexf_files:
        metrics = compute_graph_metrics(file_path)
        results.append(metrics)
    return results


def main() -> None:
    logger.info("Starting graph metrics calculation")
    script_dir = Path(__file__).resolve().parent
    abs_input_directory = (script_dir / INPUT_DIRECTORY).resolve()
    abs_output_file = (abs_input_directory / OUTPUT_FILE).resolve()

    gexf_files = get_gexf_files(abs_input_directory)
    results = process_all_graphs(gexf_files)
    if results:
        save_to_csv(results, abs_output_file)
    else:
        logger.info("No results were generated. Output CSV will not be created.")

    logger.info("Graph metrics calculation completed")


if __name__ == "__main__":
    main()