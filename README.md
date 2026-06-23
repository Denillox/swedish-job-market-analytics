# Swedish Job Market Analytics


A data engineering portfolio project analyzing Swedish Data/AI job postings from Arbetsförmedlingen's 
open JobTech API.

## Project goal

The goal is to build an end-to-end data pipeline that collects Swedish data/AI job listings, 
processes them with PySpark, stores analytics-ready datasets, and eventually visualizes job 
market trends in Power BI.


## Tech stack

- Python
- PySpark
- Docker 
- Azure Blob Storage
- Power BI 


## Pipeline

JobTech API
    ↓
Raw CSV
    ↓
PySpark transform
    ↓
Processed Parquet datasets
    ↓
Power BI dashboard planned


## Planned features

Improve skill extraction
Analyze job locations
Analyze employers
Detect remote/hybrid/on-site jobs
Upload processed data to Azure Storage
Build Power BI dashboard