# Swedish Job Market Analytics

A data engineering portfolio project analyzing Swedish Data/AI job postings from 
Arbetsförmedlingen's open JobTech API.

The project focuses on collecting job listings, processing them with PySpark, 
creating analytics-ready datasets, and eventually visualizing Swedish data/AI 
job market trends in Power BI.

## Project goal

The goal is to build an end-to-end data pipeline that answers questions such as:

Which skills are most requested in Swedish data/AI job postings?
Which cities and regions have the most data/AI jobs?
Which employers are hiring the most?
How often do job postings mention remote or hybrid work?
Which tools and cloud platforms appear most often in job descriptions?


## Current Status
Implemented:

Extract job postings from the JobTech API
Save raw job data as CSV
Run the pipeline with Docker and Docker Compose
Transform raw data with PySpark
Extract requested skills from job descriptions using keyword/regex matching
Classify job postings by keyword-based workplace type detection
Generate analytics-ready Parquet datasets

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