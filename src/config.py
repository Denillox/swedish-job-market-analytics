RAW_DATA_PATH = "data/raw/raw_jobs.csv"
JOBS_OUTPUT_PATH = "data/processed/jobs.parquet"
SKILL_COUNTS_OUTPUT_PATH = "data/processed/skill_counts.parquet"
JOB_SKILLS_OUTPUT_PATH = "data/processed/job_skills.parquet"
LOCATION_COUNTS_OUTPUT_PATH = "data/processed/location_counts.parquet"
WORKPLACE_TYPE_COUNTS_OUTPUT_PATH = "data/processed/workplace_type_counts.parquet"
EMPLOYER_COUNTS_OUTPUT_PATH = "data/processed/employer_counts.parquet"
EXPERIENCE_COUNTS_OUTPUT_PATH = "data/processed/experience_counts.parquet"
YEARS_EXPERIENCE_OUTPUT_PATH = "data/processed/years_experience_counts.parquet"

# Decrease duplicate-found words with patterns instead of single words in text search
SKILL_PATTERNS = { 
    "python": [
        r"\bpython\b",
    ],
    "sql": [
        r"\bsql\b",
        r"\bsql server\b",
        r"\bmysql\b",
        r"\bpostgresql\b",
        r"\bpostgres\b",
    ],
    "pyspark": [
        r"\bpyspark\b",
        r"\bpy spark\b",
    ],
    "spark": [
        r"(?<!py)\bspark\b",
        r"\bapache spark\b",
    ],
    "databricks": [
        r"\bdatabricks\b",
    ],
    "airflow": [
        r"\bairflow\b",
        r"\bapache airflow\b",
    ],
    "azure": [
        r"\bazure\b",
        r"\bmicrosoft azure\b",
    ],
    "aws": [
        r"\baws\b",
        r"\bamazon web services\b",
    ],
    "gcp": [
        r"\bgcp\b",
        r"\bgoogle cloud\b",
        r"\bgoogle cloud platform\b",
    ],
    "snowflake": [
        r"\bsnowflake\b",
    ],
    "dbt": [
        r"\bdbt\b",
    ],
    "power bi": [
        r"\bpower bi\b",
        r"\bpowerbi\b",
        r"\bpower-bi\b",
    ],
}