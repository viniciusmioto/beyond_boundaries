import requests
import pandas as pd
import json
import logging
import time
from typing import Dict, List

COUNTRY_CODE = "BR" 
EMAIL_FILE_PATH = "../../config/email.json"
RESULTS_PATH = "../../data/raw/publication_meta"

PUBLICATION_YEAR = [
    # "2024",
    # "2023",
    # "2022",
    # "2021",
    # "2020",
    "2019",
    "2018",
    "2017",
    "2016",
    "2015",
]


def read_email_from_json(file_path: str) -> str:
    """
    Reads the email address from a JSON file.
    """
    try:
        with open(file_path, "r") as f:
            data = json.load(f)
            email = data.get("email")
            if not email:
                raise ValueError(f"Email not found in {file_path}")
            return email
    except FileNotFoundError:
        raise FileNotFoundError(f"Email file {file_path} not found.")
    except json.JSONDecodeError:
        raise ValueError(f"Invalid JSON in {file_path}")


def setup_logging(log_file: str = "open_alex_publications.log") -> None:
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


def process_doi(doi: str) -> str:
    """
    Removes the 'https://doi.org/' prefix from a DOI string.
    """
    if doi and doi.startswith("https://doi.org/"):
        return doi.replace("https://doi.org/", "")
    return doi


def process_openalex_id(openalex_id: str) -> str:
    """
    Removes the 'https://openalex.org/' prefix from an OpenAlex ID string.
    """
    if openalex_id and openalex_id.startswith("https://openalex.org/"):
        return openalex_id.replace("https://openalex.org/", "")
    return openalex_id


def fetch_all_works(params: Dict, logger: logging.Logger) -> List[Dict]:
    """
    Fetches all works from OpenAlex API using pagination.
    """
    all_works = []
    url = "https://api.openalex.org/works"

    # Fetch first page
    logger.info("Fetching first page...")
    params["page"] = 1
    full_url = f"{url}?{'&'.join([f'{k}={v}' for k, v in params.items()])}"
    response = requests.get(full_url)
    response.raise_for_status()
    data = response.json()
    all_works.extend(data.get("results", []))

    total_count = data["meta"]["count"]
    per_page = params["per_page"]
    total_pages = (total_count + per_page - 1) // per_page  # Ceiling division
    logger.info(f"Total publications found: {total_count}")
    logger.info(f"Total pages to fetch: {total_pages}")

    # Fetch remaining pages
    for page in range(2, total_pages + 1):
        logger.info(f"Fetching page {page}/{total_pages}...")
        params["page"] = page
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            page_data = response.json()
            all_works.extend(page_data.get("results", []))
            time.sleep(3)  # Respectful delay between requests
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch page {page}: {e}")
            raise

    logger.info(f"Successfully retrieved {len(all_works)} works.")
    return all_works


def process_work(work: Dict) -> Dict:
    """
    Processes a single work to extract and flatten required fields,
    while removing DOI and OpenAlex URL prefixes.
    
    For the counts_by_year field, instead of keeping the original years,
    the citations are saved with keys representing the offset relative to the publication year.
    For example, if a work was published in 2020:
      - The number of citations in 2020 will be stored as "0_year"
      - The number of citations in 2021 will be stored as "1_year", etc.
    """
    processed = {
        "id": process_openalex_id(work.get("id")),
        "doi": process_doi(work.get("doi")),
        "title": work.get("title"),
        "publication_year": work.get("publication_year"),
        "authorships": [],
        "subfield": {},
        "cited_by_count": work.get("cited_by_count"),
        "counts_by_year": ""  # will update below
    }

    # Process authorships
    authorships = work.get("authorships", [])
    for authorship in authorships:
        author_info = authorship.get("author", {})
        institutions = [
            {
                "id": process_openalex_id(inst.get("id")),
                "display_name": inst.get("display_name"),
            }
            for inst in authorship.get("institutions", [])
        ]
        processed_author = {
            "id": process_openalex_id(author_info.get("id")),
            "name": author_info.get("display_name"),
            "institutions": institutions,
            "countries": authorship.get("countries", []),
        }
        processed["authorships"].append(processed_author)

    # Process primary topic and subfield
    primary_topic = work.get("primary_topic", {})
    processed_topic = {
        "id": process_openalex_id(primary_topic.get("id")),
        "display_name": primary_topic.get("display_name"),
    }
    processed["primary_topic"] = processed_topic

    subfield = primary_topic.get("subfield", {})
    processed["subfield"] = {
        "id": process_openalex_id(subfield.get("id")),
        "display_name": subfield.get("display_name"),
    }

    # Process counts_by_year: shift years relative to the publication year
    publication_year = work.get("publication_year")
    counts_by_year_data = work.get("counts_by_year", [])
    offset_citations = {}
    if counts_by_year_data and publication_year:
        for item in counts_by_year_data:
            # Compute offset: citation year minus publication year
            offset = item["year"] - publication_year
            if offset >= 0:
                key = f"{offset}_year"
                offset_citations[key] = item["cited_by_count"]
            # If offset is negative, skip the entry as it is likely a data error.
    processed["counts_by_year"] = json.dumps(offset_citations)

    # Convert complex fields to JSON strings
    processed["authorships"] = json.dumps(processed["authorships"])
    processed["subfield"] = json.dumps(processed["subfield"])
    return processed


def main():
    """
    Main function to execute the data retrieval and processing pipeline.
    """
    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        email = read_email_from_json(EMAIL_FILE_PATH)
        logger.info("Successfully read email address.")

        for year in PUBLICATION_YEAR:
            logger.info(f"Starting data retrieval for year {year}...")

            # Configure API parameters
            params = {
                "select": "id,doi,title,authorships,publication_year,primary_topic,cited_by_count,counts_by_year",
                "filter": "type:article,institutions.country_code:{},primary_topic.field.id:17,publication_year:{}".format(
                    COUNTRY_CODE, year
                ),
                "per_page": 25,
                "mailto": email,  # Uncomment email to be respectful
            }

            # Fetch all works
            logger.info("Starting data retrieval from OpenAlex API...")
            all_works = fetch_all_works(params, logger)

            # Process works
            logger.info("Processing retrieved works...")
            processed_works = [process_work(work) for work in all_works]

            # Create and save DataFrame
            df = pd.DataFrame(processed_works)
            df.to_csv(f"{RESULTS_PATH}/open_alex_publications_{year}.csv", index=False)
            logger.info(
                f"Data saved to 'open_alex_publications_{year}_{COUNTRY_CODE}.csv' with {len(df)} entries."
            )

    except Exception as e:
        logger.error(f"Script failed: {e}")
        raise


if __name__ == "__main__":
    main()
