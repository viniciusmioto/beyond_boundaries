import os
import networkx as nx
from utils import mappings


GRAPHS_PATH = "../../data/graphs"

# Define the range of years
YEARS = [i for i in range(2015, 2025)]

# Loop over each subfield using the keys from the SUBFIELDS_SHORT mapping
for subfield in mappings.SUBFIELDS_SHORT.keys():
    print(f"Processing subfield: {subfield}")
    # Create an empty graph; change to nx.MultiGraph() if needed
    full_graph = nx.Graph()
    
    for year in YEARS:
        filename = f"{GRAPHS_PATH}/years/{subfield}_{year}.gexf"
        if os.path.exists(filename):
            try:
                # Read the yearly GEXF graph
                yearly_graph = nx.read_gexf(filename)
                # Merge the yearly graph with the full graph
                full_graph = nx.compose(full_graph, yearly_graph)
                print(f"  Merged {filename}")
            except Exception as e:
                print(f"  Error reading {filename}: {e}")
        else:
            print(f"  File {filename} not found.")
    
    # Define the output filename
    sanitized_subfield = subfield.replace(" ", "_")
    output_filename = f"{GRAPHS_PATH}/full/{sanitized_subfield}.gexf"
    try:
        # Write the merged graph to a new GEXF file
        nx.write_gexf(full_graph, output_filename)
        print(f"Created {output_filename}\n")
    except Exception as e:
        print(f"Error writing {output_filename}: {e}")