import pandas as pd

FILES_PATH = "../../data/raw/publication_counts/subfield_publication_counts"
COUNTRIES = ["BR", "CN", "US", "IN"]
OUTPUT_FILE = "../../data/raw/publication_counts/combined_publication_counts.csv"

def main():
    # Load and store each country's DataFrame in a list
    dfs = [pd.read_csv(f"{FILES_PATH}_{country}.csv") for country in COUNTRIES]

    # Concatenate all DataFrames into a single DataFrame
    combined_df = pd.concat(dfs, ignore_index=True)

    # Save the combined DataFrame to a new CSV file
    combined_df.to_csv(OUTPUT_FILE, index=False)

    print(f"Combined dataset saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
