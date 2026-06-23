from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode, desc
from pyspark.sql.types import ArrayType, StringType

PATH = "data/raw/raw_jobs.csv"
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

    for skill in SKILLS:
        if skill in description:
            found_skills.append(skill)

    return found_skills

def read_csv(spark, path):
    df = spark.read.csv(
    path,
    header=True,
    inferSchema=True,
    multiLine=True,
    escape='"',
    )
    return df


def main():

    spark = create_spark_session()
    df = read_csv(spark, PATH)

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

    jobs_df.write.mode("overwrite").parquet("data/processed/jobs.parquet")
    skill_counts_df.write.mode("overwrite").parquet("data/processed/skill_counts.parquet")
    job_skills_df.write.mode("overwrite").parquet("data/processed/job_skills.parquet")

    print("Saved processed datasets:")
    print("- data/processed/jobs.parquet")
    print("- data/processed/skill_counts.parquet")
    print("- data/processed/job_skills.parquet")

if __name__ == "__main__":
    main()