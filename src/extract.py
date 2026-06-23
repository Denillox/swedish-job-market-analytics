import os
from datetime import datetime

import pandas as pd
import requests


BASE_URL = "https://jobsearch.api.jobtechdev.se/search"
OUTPUT_PATH = "data/raw/raw_jobs.csv"

SEARCH_TERMS = [
    "data engineer",
    "data analyst",
    "ai engineer",
    "python developer",
    "machine learning",
]


def extract_job(hit):
    return {
        "job_id": hit.get("id"),
        "job_title": hit.get("headline"),
        "employer": hit.get("employer", {}).get("name"),
        "publication_date": hit.get("publication_date"),
        "application_deadline": hit.get("application_deadline"),
        "municipality": hit.get("workplace_address", {}).get("municipality"),
        "region": hit.get("workplace_address", {}).get("region"),
        "category": hit.get("occupation", {}).get("label"),
        "type": hit.get("employment_type", {}).get("label"),
        "hours": hit.get("working_hours_type", {}).get("label"),
        "experience": hit.get("experience_required"),
        "number_of_vacancies": hit.get("number_of_vacancies"),
        "description": hit.get("description", {}).get("text"),
        "search_term": None,
    }


def fetch_jobs_for_term(term):
    jobs = []
    offset = 0
    limit = 100

    while True:
        params = {
            "q": term,
            "limit": limit,
            "offset": offset,
        }

        response = requests.get(BASE_URL, params=params, timeout=30)

        if response.status_code != 200:
            print(f"API error for '{term}': {response.status_code}")
            break

        data = response.json()
        hits = data.get("hits", [])

        if not hits:
            break

        for hit in hits:
            job = extract_job(hit)
            job["search_term"] = term
            jobs.append(job)

        offset += limit
        total = data.get("total", {}).get("value", 0)

        print(f"Fetched {len(jobs)} jobs for '{term}'")

        if offset >= total:
            break

    return jobs


def save_jobs(jobs, output_path):
    df = pd.DataFrame(jobs)
    df = df.drop_duplicates(subset="job_id")
    df["collected_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    output_dir = os.path.dirname(output_path)
    os.makedirs(output_dir, exist_ok=True)

    df.to_csv(output_path, index=False)

    print(f"Total unique jobs fetched: {len(df)}")
    print(f"Saved to {output_path}")


def main():
    all_jobs = []

    for term in SEARCH_TERMS:
        term_jobs = fetch_jobs_for_term(term)
        all_jobs.extend(term_jobs)

    save_jobs(all_jobs, OUTPUT_PATH)


if __name__ == "__main__":
    main()