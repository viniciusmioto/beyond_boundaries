import pandas as pd

FILES_PATH = "../../data/raw/publication_meta/openalex_publications"
OUTPUT_FILE = "../../data/raw/publication_meta/br_publication_meta.csv"

PUBLICATION_YEAR = [
    "2024",
    "2023",
    "2022",
    "2021",
    "2020",
    "2019",
    "2018",
    "2017",
    "2016",
    "2015"
]

def main():
    # Load and store each year's DataFrame in a list
    dfs = [pd.read_csv(f"{FILES_PATH}_{year}.csv") for year in PUBLICATION_YEAR]

    # Concatenate all DataFrames into a single DataFrame
    combined_df = pd.concat(dfs, ignore_index=True)

    # Save the combined DataFrame to a new CSV file
    combined_df.to_csv(OUTPUT_FILE, index=False)

    print(f"Combined dataset saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
