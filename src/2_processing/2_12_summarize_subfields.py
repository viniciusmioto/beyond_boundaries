import pandas as pd
import networkx as nx
import json
import os
import ast

PUBLICATIONS_FILE = "../../data/raw/publication_meta/br_publication_meta.csv"
NETWORKS_PATH = "../../data/graphs/full/"


def count_publications_per_subfield(publications_df: pd.DataFrame):
    # Helper function to extract display_name
    def extract_display_name(x):
        return json.loads(x).get("display_name")

    # Create a new column with the extracted display_name.
    publications_df["display_name"] = publications_df["subfield"].apply(
        extract_display_name
    )

    # Count the occurrences of each display_name.
    counts_df = publications_df["display_name"].value_counts().to_frame(name="count")

    return counts_df


def read_networks(networks_path: str) -> dict[str, nx.Graph]:
    network_files = os.listdir(networks_path)
    networks = {}

    for network in network_files:
        subfield = network.replace(".gexf", "")
        subfield = subfield.replace("_", " ")
        networks.__setitem__(f"{subfield}", nx.read_gexf(NETWORKS_PATH + network))

    return networks


def get_network_metrics(networks: dict[str, nx.Graph]):
    metrics = {}

    for subfield, network in networks.items():
        metrics.__setitem__(subfield, [network.number_of_nodes(), network.number_of_edges()])

    
    network_metrics_df = pd.DataFrame.from_dict(data=metrics, orient='index', columns=["authors", "edges"])
    print(network_metrics_df)


def main():
    # publications_df = pd.read_csv(PUBLICATIONS_FILE)
    # summary_df = count_publications_per_subfield(publications_df)

    networks = read_networks(NETWORKS_PATH)
    get_network_metrics(networks)


if __name__ == "__main__":
    main()
