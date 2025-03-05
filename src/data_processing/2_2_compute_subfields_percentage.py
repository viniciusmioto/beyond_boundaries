import pandas as pd


PUBLICATION_COUNTS_FILE_PATH = (
    "../../data/raw/publication_counts/combined_publication_counts.csv"
)
OUTPUT_FILE = "../../data/processed/2_subfield_percentages.csv"


def generate_subfield_percentages(df):
    """
    Computes the percentage of publications for each subfield per year and country.

    For each row in the input DataFrame:
      - Ensures 'publication_year' is an integer.
      - Computes the total publications for each country and year.
      - Calculates the percentage as (count / total) * 100.

    The resulting DataFrame (with an extra 'total' and 'percentage' column) is then
    saved to a CSV file named "subfield_percentages.csv".

    Parameters:
        df (pd.DataFrame): DataFrame with columns including "publication_year", "country_code",
                           "subfield_display_name", and "count".

    Returns:
        pd.DataFrame: The updated DataFrame with percentage calculations.
    """
    # Ensure publication_year is integer
    df["publication_year"] = df["publication_year"].astype(int)

    # Compute the total publications per country per year
    total_per_year = (
        df.groupby(["country_code", "publication_year"])["count"]
        .sum()
        .reset_index(name="total")
    )
    # Merge the totals back into the original DataFrame
    df = df.merge(total_per_year, on=["country_code", "publication_year"])

    # Calculate percentage for each row
    df["percentage"] = (df["count"] / df["total"]) * 100

    return df


def main():
    publication_counts = pd.read_csv(PUBLICATION_COUNTS_FILE_PATH)

    subfilds_percent = generate_subfield_percentages(publication_counts)

    # Save the DataFrame to CSV
    subfilds_percent.to_csv(OUTPUT_FILE, index=False)


if __name__ == "__main__":
    main()
