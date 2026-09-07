import dlt
from pyspark.sql.functions import col, row_number, max, min, first
from pyspark.sql.window import Window

@dlt.table(
    name="jobpulse_gold.dim_company",
    comment="Company dimension table with surrogate keys",
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"}
)
def dim_company():
    """
    Create company dimension with surrogate key.
    Generates unique company_key for each distinct company_name.
    """
    return (
        dlt.read("workspace.jobpulse_silver.silver_jobs")
        .groupBy("company_name")
        .agg(
            first(col("company_website"), ignorenulls=True).alias("company_website"),
            max("company_active_jobs_count").alias("total_active_jobs"),
            min("published_at").alias("first_seen_date"),
            max("published_at").alias("last_updated")
        )
        .withColumn("row_num", row_number().over(Window.orderBy("company_name")))
        .withColumn("company_key", col("row_num"))
        .select("company_key", "company_name", "company_website", "total_active_jobs", "first_seen_date", "last_updated")
    )