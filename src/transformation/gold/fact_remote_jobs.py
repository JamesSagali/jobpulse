import dlt
from pyspark.sql.functions import col, to_date, when, coalesce, lit

@dlt.table(
    name="jobpulse_gold.fact_remote_jobs",
    comment="Fact table for remote jobs with foreign keys to dimensions",
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"}
)
def fact_remote_jobs():
    """
    Create fact table for remote jobs.
    Joins silver_jobs with dimension tables to get surrogate keys.
    Includes measures like salary values and flags.
    """
    # Read source and dimensions
    silver = dlt.read("workspace.jobpulse_silver.silver_jobs").filter(col("is_remote") == True)
    dim_company = dlt.read("jobpulse_gold.dim_company")
    dim_location = dlt.read("jobpulse_gold.dim_location")
    dim_employment = dlt.read("jobpulse_gold.dim_employment_type")
    dim_date = dlt.read("jobpulse_gold.dim_date")
    dim_salary = dlt.read("jobpulse_gold.dim_salary")
    
    # Join with dimensions to get surrogate keys
    fact = (
        silver
        .join(dim_company, "company_name", "left")
        .join(dim_location, "location_full", "left")
        .join(dim_employment, "employment_type", "left")
        .join(
            dim_date,
            to_date(silver.published_at) == dim_date.full_date,
            "left"
        )
    )
    
    # Compute salary midpoint
    fact = fact.withColumn(
        "salary_mid",
        (col("salary_min") + col("salary_max")) / 2
    )
    
    # Assign salary band key based on salary_mid
    fact = fact.withColumn(
        "salary_key",
        when(col("salary_mid").isNull(), 1)
        .when(col("salary_mid") < 50000, 2)
        .when(col("salary_mid") < 75000, 3)
        .when(col("salary_mid") < 100000, 4)
        .when(col("salary_mid") < 125000, 5)
        .when(col("salary_mid") < 150000, 6)
        .when(col("salary_mid") < 175000, 7)
        .when(col("salary_mid") < 200000, 8)
        .when(col("salary_mid") < 250000, 9)
        .when(col("salary_mid") < 300000, 10)
        .when(col("salary_mid") < 400000, 11)
        .otherwise(12)
    )
    
    # Add flag for salary information availability
    fact = fact.withColumn(
        "has_salary_info",
        col("salary_mid").isNotNull()
    )
    
    return fact.select(
        col("id"),
        col("title").alias("job_title"),
        col("url").alias("job_url"),
        col("description"),
        col("company_key"),
        col("location_key"),
        col("employment_type_key"),
        col("date_key"),
        col("salary_key"),
        col("salary_min"),
        col("salary_max"),
        col("salary_mid"),
        col("has_salary_info"),
        col("is_remote"),
        col("published_at")
    )