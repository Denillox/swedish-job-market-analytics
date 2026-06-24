from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode, desc
from pyspark.sql.types import ArrayType, StringType, IntegerType
import re
from pathlib import Path
import shutil

from config import (
    RAW_DATA_PATH,
    JOBS_OUTPUT_PATH,
    SKILL_COUNTS_OUTPUT_PATH,
    JOB_SKILLS_OUTPUT_PATH,
    LOCATION_COUNTS_OUTPUT_PATH,
    WORKPLACE_TYPE_COUNTS_OUTPUT_PATH,
    EMPLOYER_COUNTS_OUTPUT_PATH,
    EXPERIENCE_COUNTS_OUTPUT_PATH,
    YEARS_EXPERIENCE_OUTPUT_PATH,
    SKILL_COUNTS_EXPORT_PATH,
    LOCATION_COUNTS_EXPORT_PATH,
    WORKPLACE_TYPE_COUNTS_EXPORT_PATH,
    EMPLOYER_COUNTS_EXPORT_PATH,
    EXPERIENCE_COUNTS_EXPORT_PATH,
    YEARS_EXPERIENCE_EXPORT_PATH,
    SKILL_PATTERNS,
    PIPELINE_SUMMARY_EXPORT_PATH,
)

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

def extract_years_experience(description):
    if description is None:
        return None

    description = description.lower()

    patterns = [
        r"minst\s+(\d+)\s+års erfarenhet",
        r"minst\s+(\d+)\s+år",
        r"(\d+)\s*\+\s*(?:års erfarenhet|years)",
        r"(\d+)\s+(?:års erfarenhet|år erfarenhet)",
        r"(\d+)\s+års\s+relevant\s+erfarenhet",
        r"(\d+)\s+års\s+arbetslivserfarenhet",
        r"(\d+)\s+år\s+inom",
        r"at least\s+(\d+)\s+years",
        r"minimum\s+(\d+)\s+years",
        r"(\d+)\s+years(?: of)? experience",
    ]

    for pattern in patterns:
        match = re.search(pattern, description)
        if match:
            years = int(match.group(1))

            if years > 20:
                return None

            return years

    return None

def write_single_csv(df, output_path):
    output_path = Path(output_path)
    temp_dir = output_path.parent / f"{output_path.stem}_tmp"

    output_path.parent.mkdir(parents=True, exist_ok=True)

    if temp_dir.exists():
        shutil.rmtree(temp_dir)

    if output_path.exists():
        output_path.unlink()

    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(str(temp_dir))
    )

    part_files = list(temp_dir.glob("part-*.csv"))

    if not part_files:
        raise FileNotFoundError(f"No CSV part file found in {temp_dir}")

    shutil.move(str(part_files[0]), str(output_path))
    shutil.rmtree(temp_dir)

    print(f"Saved CSV export: {output_path}")

def main():

    spark = create_spark_session()
    df = read_raw_jobs(spark, RAW_DATA_PATH)

    skill_udf = udf(extract_skills, ArrayType(StringType()))
    workplace_type_udf = udf(classify_workplace_type, StringType())
    years_experience_udf = udf(extract_years_experience, IntegerType())

    processed_df = (
        df
        .withColumn("skills", skill_udf(col("description")))
        .withColumn("workplace_type", workplace_type_udf(col("description")))
        .withColumn("years_experience", years_experience_udf(col("description")))
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
        "workplace_type",
        "years_experience"
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

    years_experience_counts_df = (
        jobs_df
        .groupBy("years_experience")
        .count()
        .orderBy("years_experience")
    )

    total_jobs = jobs_df.count()
    total_job_skill_rows = job_skills_df.count()
    total_unique_skills = skill_counts_df.count()
    total_locations = location_counts_df.count()
    total_employers = employer_counts_df.count()

    pipeline_summary_df = spark.createDataFrame(
    [
        (
            total_jobs,
            total_job_skill_rows,
            total_unique_skills,
            total_locations,
            total_employers,
        )
    ],
    [
        "total_jobs",
        "total_job_skill_rows",
        "total_unique_skills",
        "total_locations",
        "total_employers",
    ]
    )

    print("Top job locations:")
    location_counts_df.show(20, truncate=False)

    print("Top requested skills:")
    skill_counts_df.show(truncate=False)

    print(f"Total jobs: {total_jobs}")
    print(f"Total job-skill rows: {total_job_skill_rows}")
    print(f"Total unique skills: {total_unique_skills}")
    print(f"Total locations: {total_locations}")
    print(f"Total employers: {total_employers}")

    print("Workplace type counts:")
    workplace_type_counts_df.show(truncate=False)

    print("Employer counts:")
    employer_counts_df.show(truncate=False)

    print("Experience requirement counts:")
    experience_counts_df.show(truncate=False)

    print("Years of experience required:")
    years_experience_counts_df.show(truncate=False)

    jobs_df.write.mode("overwrite").parquet(JOBS_OUTPUT_PATH)
    skill_counts_df.write.mode("overwrite").parquet(SKILL_COUNTS_OUTPUT_PATH)
    job_skills_df.write.mode("overwrite").parquet(JOB_SKILLS_OUTPUT_PATH)
    location_counts_df.write.mode("overwrite").parquet(LOCATION_COUNTS_OUTPUT_PATH)
    workplace_type_counts_df.write.mode("overwrite").parquet(WORKPLACE_TYPE_COUNTS_OUTPUT_PATH)
    employer_counts_df.write.mode("overwrite").parquet(EMPLOYER_COUNTS_OUTPUT_PATH)
    experience_counts_df.write.mode("overwrite").parquet(EXPERIENCE_COUNTS_OUTPUT_PATH)
    years_experience_counts_df.write.mode("overwrite").parquet(YEARS_EXPERIENCE_OUTPUT_PATH)

    print("Saved processed datasets")


    write_single_csv(skill_counts_df, SKILL_COUNTS_EXPORT_PATH)
    write_single_csv(location_counts_df, LOCATION_COUNTS_EXPORT_PATH)
    write_single_csv(workplace_type_counts_df, WORKPLACE_TYPE_COUNTS_EXPORT_PATH)
    write_single_csv(employer_counts_df, EMPLOYER_COUNTS_EXPORT_PATH)
    write_single_csv(experience_counts_df, EXPERIENCE_COUNTS_EXPORT_PATH)
    write_single_csv(years_experience_counts_df, YEARS_EXPERIENCE_EXPORT_PATH)
    write_single_csv(pipeline_summary_df, PIPELINE_SUMMARY_EXPORT_PATH)

if __name__ == "__main__":
    main()