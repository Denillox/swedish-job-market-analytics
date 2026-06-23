# Swedish Job Market Analytics

A data engineering portfolio project analyzing Swedish Data/AI job postings from Arbetsförmedlingen's open JobTech API.

The project focuses on collecting job listings, processing them with PySpark, creating analytics-ready datasets, and eventually visualizing Swedish Data/AI job market trends in Power BI.

## Project Goal

The goal is to build an end-to-end data pipeline that answers questions such as:

* Which skills are most requested in Swedish Data/AI job postings?
* Which cities and regions have the most Data/AI jobs?
* Which employers are hiring the most?
* How often do job postings mention remote or hybrid work?
* Which tools and cloud platforms appear most often in job descriptions?
* How often do job postings explicitly mention years of experience?

## Current Status

Implemented:

* Extract job postings from the JobTech API
* Save raw job data as CSV
* Run the pipeline with Docker and Docker Compose
* Transform raw data with PySpark
* Extract requested skills from job descriptions using keyword/regex matching
* Classify job postings by keyword-based workplace type detection
* Extract explicit years of experience from job descriptions
* Generate analytics-ready Parquet datasets
* Run data quality checks on processed outputs
* Upload raw and processed datasets to Azure Blob Storage

## Tech Stack

* Python
* Pandas
* PySpark
* Docker
* Docker Compose
* Parquet
* Azure Blob Storage
* Power BI

## Project Structure

```text
swedish-job-market-analytics/
├── src/
│   ├── extract.py          # Fetches job postings from JobTech API
│   ├── transform.py        # Transforms raw jobs into analytics-ready datasets
│   ├── quality_checks.py   # Runs data quality checks on processed outputs
│   └── config.py           # Paths and extraction patterns
├── data/                   # Generated raw and processed data, not committed
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Pipeline

```text
JobTech API
    ↓
Raw CSV
    ↓
PySpark transform
    ↓
Processed Parquet datasets
    ↓
Power BI dashboard planned
```

## Processed Datasets

The transformation step currently creates:

| Dataset                           | Description                                            |
| --------------------------------- | ------------------------------------------------------ |
| `jobs.parquet`                    | One row per job posting                                |
| `job_skills.parquet`              | One row per job-skill pair                             |
| `skill_counts.parquet`            | Aggregated count of requested skills                   |
| `location_counts.parquet`         | Job counts by region and municipality                  |
| `workplace_type_counts.parquet`   | Keyword-based remote, hybrid, and not specified counts |
| `employer_counts.parquet`         | Job counts by employer                                 |
| `experience_counts.parquet`       | Counts based on the API-level experience required flag |
| `years_experience_counts.parquet` | Counts of explicitly mentioned years of experience     |

## How to Run

Run the extraction step:

```bash
docker compose run --rm extract
```

Run the transformation step:

```bash
docker compose run --rm transform
```

Run data quality checks:

```bash
docker compose run --rm quality
```

Run the upload step:

```bash
docker compose run --rm upload
```

Generated raw and processed data is written to the local `data/` directory.

## Data Notes

The data is collected from Arbetsförmedlingen's open JobTech API.

Skill extraction, workplace type classification, and years-of-experience extraction are currently keyword/regex-based. These outputs are useful for trend analysis, but should not be interpreted as perfect classifications.

Generated data files are excluded from the repository and are not committed to Git.

## Planned Features

* Improve skill extraction and reduce false positives
* Investigate missing location data
* Improve remote, hybrid, and on-site classification
* Upload processed data to Azure Storage
* Build a Power BI dashboard
* Add dashboard screenshots
* Add a final architecture diagram
