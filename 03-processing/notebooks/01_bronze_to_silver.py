# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze to Silver Processing
# MAGIC 
# MAGIC Transform raw job posting JSON into cleaned, validated Parquet files.
# MAGIC 
# MAGIC **Pipeline Steps:**
# MAGIC 1. Read raw JSON from Bronze layer
# MAGIC 2. Parse and flatten nested structures
# MAGIC 3. Clean and validate data
# MAGIC 4. Deduplicate records
# MAGIC 5. Extract skills from descriptions
# MAGIC 6. Normalize locations and titles
# MAGIC 7. Write to Silver layer as Parquet

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    BooleanType, ArrayType, TimestampType, DateType
)
import re
from datetime import datetime

# Initialize Spark (Databricks provides this automatically)
# For local testing, uncomment:
# spark = SparkSession.builder \
#     .appName("JobAnalytics-BronzeToSilver") \
#     .config("spark.sql.adaptive.enabled", "true") \
#     .getOrCreate()

# Configuration - Update these paths!
CONFIG = {
    "bronze_path": "s3a://job-analytics-bronze-xxxx/job_postings/",
    "silver_path": "s3a://job-analytics-silver-xxxx/jobs_cleaned/",
    "processing_date": datetime.now().strftime("%Y-%m-%d")
}

# For local testing
LOCAL_CONFIG = {
    "bronze_path": "./data/raw/",
    "silver_path": "./data/silver/",
    "processing_date": datetime.now().strftime("%Y-%m-%d")
}

# Uncomment for local testing
# CONFIG = LOCAL_CONFIG

print(f"Processing Date: {CONFIG['processing_date']}")
print(f"Bronze Path: {CONFIG['bronze_path']}")
print(f"Silver Path: {CONFIG['silver_path']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Definition

# COMMAND ----------

# Expected schema for raw job postings
raw_schema = StructType([
    StructField("job_id", StringType(), False),
    StructField("title", StringType(), False),
    StructField("company", StringType(), False),
    StructField("location", StringType(), True),
    StructField("description", StringType(), True),
    StructField("source", StringType(), False),
    StructField("scraped_at", StringType(), True),
    StructField("posted_date", StringType(), True),
    StructField("salary_min", DoubleType(), True),
    StructField("salary_max", DoubleType(), True),
    StructField("salary_currency", StringType(), True),
    StructField("job_type", StringType(), True),
    StructField("remote", BooleanType(), True),
    StructField("url", StringType(), True),
    StructField("skills", ArrayType(StringType()), True),
    StructField("raw_data", StringType(), True)  # JSON string
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Raw Data

# COMMAND ----------

def read_bronze_data(spark, path: str) -> "DataFrame":
    """Read raw JSON Lines files from Bronze layer."""
    
    df = spark.read \
        .option("multiLine", "false") \
        .json(path)
    
    record_count = df.count()
    print(f"✓ Read {record_count:,} records from Bronze layer")
    
    return df

# Read data
df_raw = read_bronze_data(spark, CONFIG["bronze_path"])
df_raw.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Parse and Flatten

# COMMAND ----------

def parse_and_flatten(df: "DataFrame") -> "DataFrame":
    """Parse timestamps and flatten any nested structures."""
    
    df_parsed = df \
        .withColumn(
            "scraped_at",
            F.to_timestamp(F.col("scraped_at"))
        ) \
        .withColumn(
            "posted_date",
            F.to_date(F.col("posted_date"))
        ) \
        .withColumn(
            "salary_min",
            F.col("salary_min").cast(DoubleType())
        ) \
        .withColumn(
            "salary_max",
            F.col("salary_max").cast(DoubleType())
        )
    
    print("✓ Parsed timestamps and numeric fields")
    return df_parsed

df_parsed = parse_and_flatten(df_raw)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Clean and Validate

# COMMAND ----------

def clean_and_validate(df: "DataFrame") -> "DataFrame":
    """Clean data and apply validation rules."""
    
    # Remove records with null required fields
    df_valid = df.filter(
        F.col("job_id").isNotNull() &
        F.col("title").isNotNull() &
        F.col("company").isNotNull()
    )
    
    # Clean text fields
    df_cleaned = df_valid \
        .withColumn(
            "title",
            F.trim(F.regexp_replace(F.col("title"), r"\s+", " "))
        ) \
        .withColumn(
            "company",
            F.trim(F.regexp_replace(F.col("company"), r"\s+", " "))
        ) \
        .withColumn(
            "location",
            F.when(
                F.col("location").isNotNull(),
                F.trim(F.regexp_replace(F.col("location"), r"\s+", " "))
            ).otherwise(F.lit("Unknown"))
        )
    
    # Validate salary ranges
    df_salary_valid = df_cleaned \
        .withColumn(
            "salary_min",
            F.when(
                (F.col("salary_min") > 0) & (F.col("salary_min") < 1000000),
                F.col("salary_min")
            ).otherwise(F.lit(None))
        ) \
        .withColumn(
            "salary_max",
            F.when(
                (F.col("salary_max") > 0) & (F.col("salary_max") < 1000000),
                F.col("salary_max")
            ).otherwise(F.lit(None))
        ) \
        .withColumn(
            "salary_max",
            F.when(
                F.col("salary_max") < F.col("salary_min"),
                F.col("salary_min")  # Swap if max < min
            ).otherwise(F.col("salary_max"))
        )
    
    removed_count = df.count() - df_salary_valid.count()
    print(f"✓ Cleaned data, removed {removed_count:,} invalid records")
    
    return df_salary_valid

df_cleaned = clean_and_validate(df_parsed)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Deduplicate

# COMMAND ----------

def deduplicate(df: "DataFrame") -> "DataFrame":
    """Remove duplicate job postings, keeping most recent."""
    
    # Dedup by job_id, keeping latest scraped_at
    from pyspark.sql.window import Window
    
    window = Window.partitionBy("job_id").orderBy(F.col("scraped_at").desc())
    
    df_dedup = df \
        .withColumn("row_num", F.row_number().over(window)) \
        .filter(F.col("row_num") == 1) \
        .drop("row_num")
    
    removed_count = df.count() - df_dedup.count()
    print(f"✓ Removed {removed_count:,} duplicate records")
    
    return df_dedup

df_dedup = deduplicate(df_cleaned)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Extract Skills

# COMMAND ----------

# Skills dictionary for extraction
SKILLS_KEYWORDS = [
    # Languages
    "python", "sql", "java", "scala", "go", "rust", "javascript", "typescript",
    "r", "julia", "bash", "shell",
    
    # Data Processing
    "spark", "pyspark", "hadoop", "hive", "presto", "trino", "flink",
    "kafka", "kinesis", "pubsub", "rabbitmq",
    
    # Databases
    "postgresql", "postgres", "mysql", "mongodb", "redis", "elasticsearch",
    "cassandra", "dynamodb", "bigquery", "redshift", "snowflake",
    
    # Cloud & Infrastructure
    "aws", "azure", "gcp", "google cloud", "terraform", "pulumi",
    "docker", "kubernetes", "k8s", "airflow", "dagster", "prefect",
    
    # Data Tools
    "dbt", "fivetran", "airbyte", "stitch", "matillion",
    "pandas", "numpy", "polars", "dask", "ray",
    
    # ML/AI
    "machine learning", "deep learning", "tensorflow", "pytorch",
    "scikit-learn", "mlflow", "kubeflow", "sagemaker",
    
    # Visualization
    "tableau", "power bi", "looker", "metabase", "superset",
    
    # Concepts
    "etl", "elt", "data warehouse", "data lake", "lakehouse",
    "data modeling", "dimensional modeling", "star schema",
    "ci/cd", "git", "agile", "scrum"
]

@F.udf(ArrayType(StringType()))
def extract_skills_udf(description: str) -> list:
    """Extract skills from job description."""
    if not description:
        return []
    
    description_lower = description.lower()
    found_skills = []
    
    for skill in SKILLS_KEYWORDS:
        # Word boundary matching
        pattern = r'\b' + re.escape(skill) + r'\b'
        if re.search(pattern, description_lower):
            found_skills.append(skill)
    
    return list(set(found_skills))


def enrich_skills(df: "DataFrame") -> "DataFrame":
    """Extract skills from descriptions if not already present."""
    
    df_skills = df.withColumn(
        "skills",
        F.when(
            (F.col("skills").isNull()) | (F.size(F.col("skills")) == 0),
            extract_skills_udf(F.col("description"))
        ).otherwise(F.col("skills"))
    )
    
    # Add skill count
    df_skills = df_skills.withColumn(
        "skill_count",
        F.size(F.col("skills"))
    )
    
    avg_skills = df_skills.agg(F.avg("skill_count")).collect()[0][0]
    print(f"✓ Extracted skills, average {avg_skills:.1f} skills per job")
    
    return df_skills

df_skills = enrich_skills(df_dedup)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Normalize Locations and Titles

# COMMAND ----------

# Title normalization mapping
TITLE_NORMALIZATIONS = {
    r"sr\.?\s*": "Senior ",
    r"jr\.?\s*": "Junior ",
    r"lead\s*": "Lead ",
    r"\bde\b": "Data Engineer",
    r"\bda\b": "Data Analyst",
    r"\bds\b": "Data Scientist",
    r"\bmle\b": "Machine Learning Engineer",
    r"\bswe\b": "Software Engineer",
    r"\s+": " "  # Multiple spaces to single
}

@F.udf(StringType())
def normalize_title_udf(title: str) -> str:
    """Normalize job title."""
    if not title:
        return title
    
    normalized = title
    for pattern, replacement in TITLE_NORMALIZATIONS.items():
        normalized = re.sub(pattern, replacement, normalized, flags=re.IGNORECASE)
    
    return normalized.strip().title()


@F.udf(StringType())
def extract_city_udf(location: str) -> str:
    """Extract city from location string."""
    if not location:
        return None
    
    # Handle "City, State" or "City, State, Country"
    parts = [p.strip() for p in location.split(",")]
    return parts[0] if parts else None


@F.udf(StringType())
def extract_state_udf(location: str) -> str:
    """Extract state from location string."""
    if not location:
        return None
    
    parts = [p.strip() for p in location.split(",")]
    if len(parts) >= 2:
        return parts[1]
    return None


@F.udf(BooleanType())
def is_remote_udf(location: str, title: str) -> bool:
    """Determine if job is remote."""
    text = f"{location or ''} {title or ''}".lower()
    remote_keywords = ["remote", "work from home", "wfh", "distributed", "anywhere"]
    return any(kw in text for kw in remote_keywords)


def normalize_data(df: "DataFrame") -> "DataFrame":
    """Normalize titles and locations."""
    
    df_normalized = df \
        .withColumn("title_normalized", normalize_title_udf(F.col("title"))) \
        .withColumn("city", extract_city_udf(F.col("location"))) \
        .withColumn("state", extract_state_udf(F.col("location"))) \
        .withColumn("country", F.lit("USA"))  # Default, can be enhanced \
        .withColumn(
            "is_remote",
            F.when(
                F.col("remote").isNotNull(),
                F.col("remote")
            ).otherwise(is_remote_udf(F.col("location"), F.col("title")))
        )
    
    print("✓ Normalized titles and locations")
    return df_normalized

df_normalized = normalize_data(df_skills)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Write to Silver Layer

# COMMAND ----------

def write_to_silver(df: "DataFrame", output_path: str, processing_date: str):
    """Write processed data to Silver layer as Parquet."""
    
    # Add processing metadata
    df_final = df \
        .withColumn("processing_date", F.lit(processing_date).cast(DateType())) \
        .withColumn("processing_timestamp", F.current_timestamp())
    
    # Select final columns in order
    columns = [
        "job_id",
        "title",
        "title_normalized",
        "company",
        "location",
        "city",
        "state",
        "country",
        "is_remote",
        "salary_min",
        "salary_max",
        "salary_currency",
        "job_type",
        "description",
        "skills",
        "skill_count",
        "posted_date",
        "scraped_at",
        "source",
        "url",
        "processing_date",
        "processing_timestamp"
    ]
    
    df_output = df_final.select([c for c in columns if c in df_final.columns])
    
    # Write partitioned by processing date
    df_output \
        .coalesce(4) \
        .write \
        .mode("overwrite") \
        .partitionBy("processing_date") \
        .parquet(output_path)
    
    record_count = df_output.count()
    print(f"✓ Wrote {record_count:,} records to Silver layer")
    print(f"  Path: {output_path}")
    
    return df_output

df_silver = write_to_silver(
    df_normalized, 
    CONFIG["silver_path"],
    CONFIG["processing_date"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Summary

# COMMAND ----------

# Print summary statistics
print("\n" + "="*60)
print("PROCESSING SUMMARY")
print("="*60)

print(f"\nRecord Counts:")
print(f"  Bronze (raw):     {df_raw.count():,}")
print(f"  Silver (cleaned): {df_silver.count():,}")

print(f"\nData Quality:")
null_salary = df_silver.filter(F.col("salary_min").isNull()).count()
print(f"  Jobs with salary: {df_silver.count() - null_salary:,} ({100*(df_silver.count()-null_salary)/df_silver.count():.1f}%)")

remote_count = df_silver.filter(F.col("is_remote") == True).count()
print(f"  Remote jobs:      {remote_count:,} ({100*remote_count/df_silver.count():.1f}%)")

avg_skills = df_silver.agg(F.avg("skill_count")).collect()[0][0]
print(f"  Avg skills/job:   {avg_skills:.1f}")

print(f"\nTop 10 Skills:")
df_silver \
    .select(F.explode("skills").alias("skill")) \
    .groupBy("skill") \
    .count() \
    .orderBy(F.desc("count")) \
    .limit(10) \
    .show()

print(f"\nTop 10 Companies:")
df_silver \
    .groupBy("company") \
    .count() \
    .orderBy(F.desc("count")) \
    .limit(10) \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC 1. **Run Glue Crawler** to update catalog
# MAGIC 2. **Execute dbt models** for Gold layer transformation
# MAGIC 3. **Run Great Expectations** for data quality validation
