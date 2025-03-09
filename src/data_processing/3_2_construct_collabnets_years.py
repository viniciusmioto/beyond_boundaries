import json
import pandas as pd
import networkx as nx
from itertools import combinations
from collections import defaultdict
import os
from utils import mappings

INPUT_PATH = "../../data/raw/publication_meta/br_publication_meta.csv"
OUTPUT_PATH = "../../data/graphs/years"

def parse_json_field(field_str):
    try:
        return json.loads(field_str)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return None

def get_author_country(author):
    countries = author.get("countries", [])
    if countries:
        return countries[0]
    return "Unknown"

def update_author_subfield_counts(author_subfield_counts, authors, subfield_name):
    for author in authors:
        author_id = author.get("id")
        if author_id:
            author_subfield_counts[author_id][subfield_name] += 1

def determine_primary_subfield(subfield_count):
    if not subfield_count:
        return "Unknown"
    return max(subfield_count.items(), key=lambda x: x[1])[0]

def build_collaboration_network(df):
    author_info = {}
    author_subfield_counts = defaultdict(lambda: defaultdict(int))
    collaboration_edges = defaultdict(int)

    for idx, row in df.iterrows():
        authors = parse_json_field(row["authorships"])
        if authors is None:
            continue

        subfield_data = parse_json_field(row["subfield"])
        subfield_name = (
            subfield_data.get("display_name", "Unknown") if subfield_data else "Unknown"
        )

        for author in authors:
            author_id = author.get("id")
            if not author_id:
                continue
            if author_id not in author_info:
                country = get_author_country(author)
                author_info[author_id] = {"country": country}
            author_subfield_counts[author_id][subfield_name] += 1

        for author1, author2 in combinations(authors, 2):
            id1, id2 = author1.get("id"), author2.get("id")
            if id1 and id2:
                edge = tuple(sorted([id1, id2]))
                collaboration_edges[edge] += 1

    G = nx.Graph()
    for author_id, info in author_info.items():
        primary_subfield = determine_primary_subfield(author_subfield_counts[author_id])
        G.add_node(author_id, label1=info["country"], label2=primary_subfield)

    for (id1, id2), weight in collaboration_edges.items():
        G.add_edge(id1, id2, weight=weight)

    return G

def filter_subfield_publications(df: pd.DataFrame, subfield: str) -> pd.DataFrame:
    filtered_df = df[
        df["subfield"].apply(lambda x: json.loads(x).get("display_name") == subfield)
    ]
    return filtered_df

def filter_publications_by_year(df: pd.DataFrame, year: int) -> pd.DataFrame:
    return df[df["publication_year"] == year]

def main():
    try:
        full_df = pd.read_csv(INPUT_PATH)
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    for subfield in mappings.SUBFIELDS_SHORT.keys():
        subfield_df = filter_subfield_publications(full_df, subfield)

        for year in range(2015, 2025):
            df = filter_publications_by_year(subfield_df, year)
            G = build_collaboration_network(df)

            sanitized_subfield = subfield.replace(" ", "_")
            output_file = f"{OUTPUT_PATH}/{sanitized_subfield}_{year}.gexf"
            try:
                nx.write_gexf(G, output_file)
                print(f"Graph successfully written to {output_file}")
            except Exception as e:
                print(f"Error writing GEXF file: {e}")

if __name__ == "__main__":
    main()
