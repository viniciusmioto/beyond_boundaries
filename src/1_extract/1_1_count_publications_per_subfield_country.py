import requests
import pandas as pd
import json
import logging
import time
from typing import Dict

COUNTRY_CODE = "IN"  
EMAIL_FILE_PATH = "../../config/email.json"
SUBFIELDS_FILE_PATH = "../../data/external/openalex_unique_subfields.csv"
OUTPUT_PATH = "../../data/raw/publication_counts"

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

def read_email_from_json() -> str:
    """
    Reads the email address from a JSON file.
    OpenAlex asks for an email address for respectfull requests
    """
    try:
        with open(EMAIL_FILE_PATH, "r") as f:
            data = json.load(f)
            email = data.get("email")
            if not email:
                raise ValueError(f"Email not found in {EMAIL_FILE_PATH}")
            return email
    except FileNotFoundError:
        raise FileNotFoundError(f"Email file {EMAIL_FILE_PATH} not found.")
    except json.JSONDecodeError:
        raise ValueError(f"Invalid JSON in {EMAIL_FILE_PATH}")

def setup_logging(log_file: str = "../../logs/open_alex_publications.log") -> None:
    """
    Configures logging to both a file and the console.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Remove existing handlers to avoid duplication
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # File handler with detailed logs
    file_handler = logging.FileHandler(log_file)
    file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Console handler with simple messages
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter("%(message)s")
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

def fetch_publication_count(params: Dict, logger: logging.Logger) -> int:
    """
    Sends a query to the OpenAlex API with minimal data (per_page=1) and returns the total count
    of works from the meta section of the response.
    """
    url = "https://api.openalex.org/works"
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    count = data.get("meta", {}).get("count", 0)
    citation_count = data.get("meta", {}).get("cited_by_count_sum", 0)
    return count, citation_count

def main():
    """
    Main function to fetch publication counts per subfield and year for a specific country,
    and save the results to a CSV.
    """
    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        email = read_email_from_json()
        logger.info("Successfully read email address.")

        # Load unique subfields from CSV (assumes columns: subfield_id, subfield_display_name)
        subfields_df = pd.read_csv(SUBFIELDS_FILE_PATH)
        if subfields_df.empty:
            logger.error("No subfields loaded from unique_subfields.csv. Exiting.")
            return

        results_list = []

        # Iterate over each publication year and each subfield.
        for year in PUBLICATION_YEAR:

            for _, row in subfields_df.iterrows():
                subfield_id = row["subfield_id"]
                subfield_display_name = row["subfield_display_name"]

                # Build filter query string.
                # Adjust the filter as needed. Here, we assume primary_topic.field.id is 17.
                filter_query = (
                    f"type:types/article|types/book-chapter,"
                    f"institutions.country_code:{COUNTRY_CODE},"
                    f"primary_topic.field.id:17,"
                    f"primary_topic.subfield.id:{subfield_id},"
                    f"publication_year:{year}"
                )
                params = {
                    "select": "id",  # Only need the id field
                    "filter": filter_query,
                    "per_page": 1,  # Minimal result; we use the meta for the count,
                    "cited_by_count_sum": "true",  # Include citation count
                    "mailto": email,
                }
                try:
                    count, citation_count = fetch_publication_count(params, logger)
                    logger.info(f"Year {year}, {subfield_id} ({subfield_display_name}): count = {count}, citation_count = {citation_count}")
                    results_list.append({
                        "publication_year": year,
                        "subfield_id": subfield_id,
                        "subfield_display_name": subfield_display_name,
                        "count": count,
                        "citation_count": citation_count,
                    })
                    time.sleep(2.5)  # Delay between requests to be respectful
                except Exception as e:
                    logger.error(f"Error fetching count for year {year}, subfield {subfield_id}: {e}")

        # Create DataFrame from results and save to CSV.
        results_df = pd.DataFrame(results_list)
        results_df["country_code"] = COUNTRY_CODE
        output_csv = f"{OUTPUT_PATH}/subfield_publication_counts_{COUNTRY_CODE}.csv"
        results_df.to_csv(output_csv, index=False)
        logger.info(f"Saved publication counts to {output_csv}")

    except Exception as e:
        logger.error(f"Script failed: {e}")
        raise

if __name__ == "__main__":
    main()
