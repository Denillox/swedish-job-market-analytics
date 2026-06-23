from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc


JOBS_PATH = "data/processed/jobs.parquet"
SKILL_COUNTS_PATH = "data/processed/skill_counts.parquet"
LOCATION_COUNTS_PATH = "data/processed/location_counts.parquet"
WORKPLACE_TYPE_COUNTS_PATH = "data/processed/workplace_type_counts.parquet"
EMPLOYER_COUNTS_PATH = "data/processed/employer_counts.parquet"
EXPERIENCE_COUNTS_PATH = "data/processed/experience_counts.parquet"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("JobMarketQualityChecks")
        .getOrCreate()
    )


def main():
    spark = create_spark_session()

    jobs_df = spark.read.parquet(JOBS_PATH)
    skill_counts_df = spark.read.parquet(SKILL_COUNTS_PATH)
    location_counts_df = spark.read.parquet(LOCATION_COUNTS_PATH)
    workplace_type_counts_df = spark.read.parquet(WORKPLACE_TYPE_COUNTS_PATH)
    employer_counts_df = spark.read.parquet(EMPLOYER_COUNTS_PATH)
    experience_counts_df = spark.read.parquet(EXPERIENCE_COUNTS_PATH)

    print("Total jobs:")
    print(jobs_df.count())

    print("Jobs with missing location:")
    jobs_df.filter(
        col("region").isNull() | col("municipality").isNull()
    ).select(
        "job_id",
        "job_title",
        "employer",
        "region",
        "municipality",
        "workplace_type",
        "experience_required",
    ).show(20, truncate=False)

    print("Top requested skills:")
    skill_counts_df.show(20, truncate=False)

    print("Top locations:")
    location_counts_df.show(20, truncate=False)

    print("Workplace type counts:")
    workplace_type_counts_df.show(truncate=False)

    print("Top employers:")
    employer_counts_df.show(20, truncate=False)

    print("Experience requirement counts:")
    experience_counts_df.show(truncate=False)

    print("Sample remote/hybrid jobs:")
    jobs_df.filter(
        col("workplace_type").isin("remote", "hybrid")
    ).select(
        "job_id",
        "job_title",
        "employer",
        "municipality",
        "region",
        "workplace_type",
        "experience_required",
    ).show(30, truncate=False)

    print("Sample jobs with high years of experience requirement:")
    jobs_df.filter(
        col("years_experience") >= 10
    ).select(
        "job_id",
        "job_title",
        "employer",
        "years_experience"
    ).show(30, truncate=False)


if __name__ == "__main__":
    main()