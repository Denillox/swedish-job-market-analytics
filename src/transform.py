from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode, desc
from pyspark.sql.types import ArrayType, StringType
import re

RAW_DATA_PATH = "data/raw/raw_jobs.csv"
JOBS_OUTPUT_PATH = "data/processed/jobs.parquet"
SKILL_COUNTS_OUTPUT_PATH = "data/processed/skill_counts.parquet"
JOB_SKILLS_OUTPUT_PATH = "data/processed/job_skills.parquet"
LOCATION_COUNTS_OUTPUT_PATH = "data/processed/location_counts.parquet"
WORKPLACE_TYPE_COUNTS_OUTPUT_PATH = "data/processed/workplace_type_counts.parquet"
EMPLOYER_COUNTS_OUTPUT_PATH = "data/processed/employer_counts.parquet"
EXPERIENCE_COUNTS_OUTPUT_PATH = "data/processed/experience_counts.parquet"


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

def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("JobMarketAnalytics")
        .getOrCreate()
    )
    return spark

def extract_skills(description):
    if description is None:
        return []

    description = description.lower()
    found_skills = []

    for skill, patterns in SKILL_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, description):
                found_skills.append(skill)
                break

    return found_skills

def read_raw_jobs(spark, path):
    df = spark.read.csv(
    path,
    header=True,
    inferSchema=True,
    multiLine=True,
    escape='"',
    )
    return df

def classify_workplace_type(description):
    if description is None:
        return "unknown"

    description = description.lower()

    remote_patterns = [
        r"\bdistans\b",
        r"\bremote\b",
        r"\bremote work\b",
        r"\barbeta hemifrån\b",
        r"\bhemarbete\b",
    ]

    hybrid_patterns = [
        r"\bhybrid\b",
        r"\bhybridarbete\b",
        r"\bhybrid work\b",
    ]

    for pattern in hybrid_patterns:
        if re.search(pattern, description):
            return "hybrid"

    for pattern in remote_patterns:
        if re.search(pattern, description):
            return "remote"

    return "not_specified"


def main():

    spark = create_spark_session()
    df = read_raw_jobs(spark, RAW_DATA_PATH)

    skill_udf = udf(extract_skills, ArrayType(StringType()))
    workplace_type_udf = udf(classify_workplace_type, StringType())

    processed_df = (
        df
        .withColumn("skills", skill_udf(col("description")))
        .withColumn("workplace_type", workplace_type_udf(col("description")))
    )

    jobs_df = processed_df.select(
        "job_id",
        "job_title",
        "employer",
        "municipality",
        "region",
        "publication_date",
        "search_term",
        col("experience").alias("experience_required"),
        "skills",
        "workplace_type"
    )

    job_skills_df = jobs_df.select(
        "job_id",
        explode(col("skills")).alias("skill")
    )

    skill_counts_df = (
        job_skills_df
        .groupBy("skill")
        .count()
        .orderBy(desc("count"))
    )

    location_counts_df = (
        jobs_df
        .groupBy("region", "municipality")
        .count()
        .orderBy(desc("count"))
    )

    workplace_type_counts_df = (
        jobs_df
        .groupBy("workplace_type")
        .count()
        .orderBy(desc("count"))
    )

    employer_counts_df = (
        jobs_df
        .groupBy("employer")
        .count()
        .orderBy(desc("count"))
    )

    experience_counts_df = (
        jobs_df
        .groupBy("experience_required")
        .count()
        .orderBy(desc("count"))
    )

    print("Top job locations:")
    location_counts_df.show(20, truncate=False)

    print("Top requested skills:")
    skill_counts_df.show(truncate=False)

    print(f"Total jobs: {jobs_df.count()}")
    print(f"Total job-skill rows: {job_skills_df.count()}")

    print("Workplace type counts:")
    workplace_type_counts_df.show(truncate=False)

    print("Employer counts:")
    employer_counts_df.show(truncate=False)

    print("Experience requirement counts:")
    experience_counts_df.show(truncate=False)

    jobs_df.write.mode("overwrite").parquet(JOBS_OUTPUT_PATH)
    skill_counts_df.write.mode("overwrite").parquet(SKILL_COUNTS_OUTPUT_PATH)
    job_skills_df.write.mode("overwrite").parquet(JOB_SKILLS_OUTPUT_PATH)
    location_counts_df.write.mode("overwrite").parquet(LOCATION_COUNTS_OUTPUT_PATH)
    workplace_type_counts_df.write.mode("overwrite").parquet(WORKPLACE_TYPE_COUNTS_OUTPUT_PATH)
    employer_counts_df.write.mode("overwrite").parquet(EMPLOYER_COUNTS_OUTPUT_PATH)
    experience_counts_df.write.mode("overwrite").parquet(EXPERIENCE_COUNTS_OUTPUT_PATH)

    print("Saved processed datasets:")
    print("- data/processed/jobs.parquet")
    print("- data/processed/skill_counts.parquet")
    print("- data/processed/job_skills.parquet")
    print("- data/processed/location_counts.parquet")
    print("- data/processed/workplace_type_counts.parquet")
    print("- data/processed/employer_counts.parquet")
    print("- data/processed/experience_counts.parquet")

if __name__ == "__main__":
    main()