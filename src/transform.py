from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode, desc
from pyspark.sql.types import ArrayType, StringType


SKILLS = [
    "python",
    "sql",
    "pyspark",
    "spark",
    "databricks",
    "airflow",
    "azure",
    "aws",
    "gcp",
    "snowflake",
    "dbt",
    "power bi",
]


def extract_skills(description):
    if description is None:
        return []

    description = description.lower()
    found_skills = []

    for skill in SKILLS:
        if skill in description:
            found_skills.append(skill)

    return found_skills


spark = (
    SparkSession.builder
    .appName("JobMarketAnalytics")
    .getOrCreate()
)

df = spark.read.csv(
    "data/raw/raw_jobs.csv",
    header=True,
    inferSchema=True,
    multiLine=True,
    escape='"',
)

skill_udf = udf(extract_skills, ArrayType(StringType()))

processed_df = df.withColumn(
    "skills",
    skill_udf(col("description"))
)

jobs_df = processed_df.select(
    "job_id",
    "job_title",
    "employer",
    "municipality",
    "region",
    "publication_date",
    "search_term",
    "skills",
)

skill_counts_df = (
    jobs_df
    .select(explode(col("skills")).alias("skill"))
    .groupBy("skill")
    .count()
    .orderBy(desc("count"))
)

# jobs_df.show(5, truncate=False)
skill_counts_df.show(truncate=False)

jobs_df.write.mode("overwrite").parquet("data/processed/jobs.parquet")
skill_counts_df.write.mode("overwrite").parquet("data/processed/skill_counts.parquet")

print("Saved processed datasets:")
print("- data/processed/jobs.parquet")
print("- data/processed/skill_counts.parquet")