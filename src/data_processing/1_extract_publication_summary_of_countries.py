# https://openalex.org/works?page=1&filter=primary_topic.field.id%3Afields%2F17,publication_year%3A2015%20-%202024&group_by=publication_year,open_access.is_oa,primary_topic.id,authorships.institutions.lineage,type,authorships.countries&view=list,report,api

# According to the OpenAlex link above, the 15 top countries in terms of number of publication are listed as follows

import requests
import csv
from time import sleep

OUTPUT_FILE = "../../data/processed/1_publication_summary_of_countries.csv"

COUNTRY_CODES = {
    "CN":"China",
    "US":"United States of America",
    "IN":"India",
    "ID":"Indonesia",
    "GB":"Great Britain and Northern Ireland",
    "DE":"Germany",
    "FR":"France",
    "JP":"Japan",
    "CA":"Canada",
    "IT":"Italy",
    "RU":"Russian Federation",
    "ES":"Spain",
    "BR":"Brazil",
    "KR":"Republic of Korea",
    "AU":"Australia",
}


def get_count(url):
    """Makes a GET request to the given URL and returns the count from the response metadata."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        total_publications = data.get("meta", {}).get("count", 0)
        citations_count = data.get("meta", {}).get("cited_by_count_sum", 0)

        return total_publications, citations_count
    except Exception as e:
        print(f"Error fetching data from {url}: {e}")
        return 0, 0


def main():
    results = []

    for country in COUNTRY_CODES.keys():
        print(f"Getting publication summary for {COUNTRY_CODES[country]}")

        # Build the API endpoint using the country code
        works_url = (
            f"https://api.openalex.org/works?page=1&"
            f"filter=primary_topic.field.id:fields/17,publication_year:2019+-+2024,type:types/article|types/book-chapter,"
            f"authorships.countries:{country}&cited_by_count_sum=true&per_page=1"
        )

        total_publications, citations_count = get_count(works_url)

        results.append(
            {
                "country_code": country,
                "country": COUNTRY_CODES[country],
                "total_publications": total_publications,
                "citation_count": citations_count,
                "ratio": round(citations_count / total_publications, 2) if total_publications > 0 else 0,
            }
        )

        sleep(1)

    # Sort the results by total publications (descending order)
    results.sort(key=lambda x: x["total_publications"], reverse=True)

    # Assign rank based on sorted order
    for rank, row in enumerate(results, start=1):
        row["rank"] = rank

    # Write results to CSV
    try:
        with open(OUTPUT_FILE, "w", newline="") as csvfile:
            fieldnames = ["rank", "country_code", "country", "total_publications", "citation_count", "ratio"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        print(f"Data successfully written to {OUTPUT_FILE}")
    except Exception as e:
        print(f"Error writing CSV file: {e}")


if __name__ == "__main__":
    main()