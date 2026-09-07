from pyspark import pipelines as dp
from pyspark.sql.functions import *

# Bronze layer: Ingest raw job data from JSON files with data quality checks

RAW_PATH = "/Volumes/workspace/jobpulse_bronze/raw_startup_jobs/"

@dp.table(
    name="jobpulse_bronze.bronze_jobs",
    comment="Bronze layer table containing raw job postings from startup job feeds",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dp.expect_or_drop("valid_source_file", "_source_file IS NOT NULL")
@dp.expect_or_drop("valid_job_id", "id IS NOT NULL")
@dp.expect("valid_timestamp", "_ingested_at IS NOT NULL")
def bronze_jobs():
    """
    Ingest job data from raw JSON files in Volume storage.
    
    Data Quality Checks:
    - Drop rows without source file reference (critical for tracking)
    - Drop rows without valid job ID (ensures data integrity)
    - Warn on missing ingestion timestamp
    """
    # Read raw JSON files using Auto Loader
    raw_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .option("recursiveFileLookup", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(RAW_PATH)
        .withColumn(
            "_source_file",
            col("_metadata.file_path")
        )
    )
    
    # Flatten job records from nested array
    jobs_df = (
        raw_df
        .withColumn("job", explode("data"))
        .select(
            "job.*",
            "_source_file"
        )
        .withColumn("_source", lit("startup_jobs"))
        .withColumn("_ingested_at", current_timestamp())
    )
    
    return jobs_df
