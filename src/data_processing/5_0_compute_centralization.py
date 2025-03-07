import pandas as pd
import networkx as nx
import numpy as np
from pathlib import Path
from utils import mappings


INPUT_PATH = "../../data/graphs/years/"
OUTPUT_PATH = "../../data/processed/5_centralization_df.csv"

YEARS = [i for i in range(2015, 2025)]


def betweenness_centralization(G):
    """
    Calculate the betweenness centralization of a graph.

    Parameters:
    -----------
    G : networkx.Graph
        The input graph

    Returns:
    --------
    float
        The betweenness centralization value (between 0 and 1)
    """
    # Calculate betweenness centrality for all nodes
    betweenness_dict = nx.betweenness_centrality(G, normalized=True)

    # Get the maximum betweenness centrality value
    max_centrality = max(betweenness_dict.values()) if betweenness_dict else 0

    # Calculate the sum of differences from the maximum value
    sum_of_differences = sum(
        max_centrality - centrality for centrality in betweenness_dict.values()
    )

    # Calculate the theoretical maximum (star graph has maximum betweenness centralization)
    n = G.number_of_nodes()
    if n <= 2:
        return 0.0  # Centralization is undefined for graphs with 1 or 2 nodes

    # For a star graph with n nodes, the maximum sum of differences is (n-1)
    max_possible_difference = n - 1

    # Calculate centralization
    if max_possible_difference == 0:
        return 0.0

    centralization = sum_of_differences / max_possible_difference

    return centralization


def create_centralization_dataframe(data_dir, subfields, years):
    """
    Create a dataframe with cumulative betweenness centralization values for all subfields and years.
    The network for each year is the combined (cumulative) network from the starting year up to that year.

    Parameters:
    -----------
    data_dir : str
        Directory containing the GEXF files
    subfields : list
        List of subfield names
    years : list
        List of years

    Returns:
    --------
    pandas.DataFrame
        DataFrame with subfields as rows and years as columns (plus an 'Average' column)
    """
    # Initialize DataFrame
    df = pd.DataFrame(index=subfields, columns=years)

    # Process each subfield
    for subfield in subfields:
        # Initialize an empty cumulative graph
        cumulative_G = nx.Graph()
        for year in years:
            # Construct filename with the subfield name and year
            filename = f"{subfield}_{year}.gexf"
            filepath = Path(data_dir) / filename

            try:
                # Load the graph for the given year
                G_year = nx.read_gexf(filepath)
                # Combine with the cumulative graph (using nx.compose to merge nodes and edges)
                cumulative_G = nx.compose(cumulative_G, G_year)
                print(f"Combined: {filename}")
            except (FileNotFoundError, IOError) as e:
                print(f"Warning: Could not load {filename}: {e}")
            
            # Calculate centralization if the cumulative graph has nodes, otherwise assign NaN
            if cumulative_G.number_of_nodes() > 0:
                df.at[subfield, year] = betweenness_centralization(cumulative_G)
            else:
                df.at[subfield, year] = np.nan

    # Add an 'Average' column across years (ignoring NaN values)
    df["Average"] = df[years].mean(axis=1)

    return df


def main():
    centralization_df = create_centralization_dataframe(
        INPUT_PATH, list(mappings.SUBFIELDS_SHORT.keys()), YEARS
    )
    centralization_df.to_csv(OUTPUT_PATH)
    print(f"Centralization data frame saved to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
