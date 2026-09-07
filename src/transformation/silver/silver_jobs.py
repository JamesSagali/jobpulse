from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Silver layer: Clean, deduplicate, and enrich job data with extensive data quality checks

@dp.materialized_view(
    name="jobpulse_silver.silver_jobs",
    comment="Silver layer table with cleaned, deduplicated job postings and enriched fields",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dp.expect_or_fail("unique_job_ids", "id IS NOT NULL")
@dp.expect_or_drop("valid_company_name", "company_name IS NOT NULL AND trim(company_name) != ''")
@dp.expect("valid_published_date", "published_at IS NOT NULL AND published_at <= current_timestamp()")
@dp.expect("has_title", "title IS NOT NULL AND length(title) > 0")
@dp.expect("has_description", "description IS NOT NULL AND length(description) > 10")
@dp.expect_all({
    "valid_salary_range": "salary_min IS NULL OR salary_max IS NULL OR salary_min <= salary_max",
    "positive_salary_min": "salary_min IS NULL OR salary_min > 0",
    "positive_salary_max": "salary_max IS NULL OR salary_max > 0",
    "valid_currency": "salary_currency IS NULL OR salary_currency IN ('USD', 'CAD', 'GBP', 'PHP', 'MYR')",
    "valid_period": "salary_period IS NULL OR salary_period IN ('hour', 'year', 'month', 'week')",
    "valid_workplace": "workplace_type IN ('Remote', 'On-Site', 'Hybrid') OR workplace_type IS NULL",
    "valid_employment": "employment_type IS NOT NULL",
    "valid_country_code": "country_code IS NULL OR length(country_code) = 2",
    "has_location_info": "location_full IS NOT NULL AND location_full != 'Location Not Specified'"
})
def silver_jobs():
    """
    Transform bronze job data into clean, analytics-ready silver table.
    
    Transformations:
    - Deduplication by job ID (latest ingestion timestamp wins)
    - Timestamp conversion for published_at
    - Flatten nested company and location structures
    - HTML cleaning for job descriptions
    - Salary parsing (currency, min, max, period with proper number normalization)
    - Workplace type and location standardization
    - Role array extraction
    
    Data Quality Checks:
    - FAIL: Missing or null job IDs (critical for uniqueness)
    - DROP: Missing company name (required for analytics)
    - WARN: Invalid published dates, missing titles/descriptions
    - WARN: Salary validation (range, positivity, currency, period)
    - WARN: Workplace type, employment type, location validation
    """
    
    # Read from bronze layer
    bronze_df = spark.read.table("jobpulse_bronze.bronze_jobs")
    
    # ========================================
    # 1. DEDUPLICATION
    # ========================================
    window_spec = (
        Window
        .partitionBy("id")
        .orderBy(F.col("_ingested_at").desc())
    )
    
    deduped_df = (
        bronze_df
        .withColumn("_row_number", F.row_number().over(window_spec))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )
    
    # ========================================
    # 2. TIMESTAMP CONVERSION
    # ========================================
    silver_df = deduped_df.withColumn(
        "published_at",
        F.to_timestamp("published_at")
    )
    
    # ========================================
    # 3. FLATTEN COMPANY AND LOCATION
    # ========================================
    silver_df = (
        silver_df
        # Company fields
        .withColumn("company_name", F.col("company.name"))
        .withColumn("company_slug", F.col("company.slug"))
        .withColumn("company_website", F.col("company.website_url"))
        .withColumn("company_active_jobs_count", F.col("company.active_jobs_count"))
        # Location fields
        .withColumn("city", F.col("location.city"))
        .withColumn("state", F.col("location.state"))
        .withColumn("country", F.col("location.country"))
        .withColumn("country_code", F.col("location.country_code"))
        # Drop nested structs
        .drop("company", "location")
    )
    
    # ========================================
    # 4. HTML CLEANING
    # ========================================
    silver_df = (
        silver_df
        # Remove HTML tags
        .withColumn("description", F.regexp_replace(F.col("description_html"), "<[^>]+>", " "))
        # Replace HTML entities
        .withColumn("description", F.regexp_replace(F.col("description"), r"&nbsp;", " "))
        .withColumn("description", F.regexp_replace(F.col("description"), r"&amp;", "&"))
        .withColumn("description", F.regexp_replace(F.col("description"), r"&lt;", "<"))
        .withColumn("description", F.regexp_replace(F.col("description"), r"&gt;", ">"))
        # Normalize whitespace
        .withColumn("description", F.regexp_replace(F.col("description"), r"\s+", " "))
        .withColumn("description", F.trim(F.col("description")))
    )
    
    # ========================================
    # 5. SALARY PARSING
    # ========================================
    # Extract currency
    silver_df = silver_df.withColumn(
        "salary_currency",
        F.when(F.lower(F.col("salary")).contains("cad"), "CAD")
         .when(F.col("salary").contains("£"), "GBP")
         .when(F.col("salary").contains("₱"), "PHP")
         .when(F.col("salary").contains("RM"), "MYR")
         .when(F.col("salary").contains("$"), "USD")
    )
    
    # Extract min salary (raw)
    silver_df = silver_df.withColumn(
        "salary_min_raw",
        F.regexp_extract(F.col("salary"), r"(?i)(?:CAD\s*)?\$?\s*[£₱€]?\s*([\d.,]+)", 1)
    )
    
    # Extract max salary (raw)
    silver_df = silver_df.withColumn(
        "salary_max_raw",
        F.regexp_extract(F.col("salary"), r"(?i)[–—-]\s*(?:CAD\s*)?\$?\s*[£₱€]?\s*([\d.,]+)", 1)
    )
    
    # Extract salary period
    silver_df = silver_df.withColumn(
        "salary_period",
        F.when(F.lower(F.col("salary")).contains("hour"), "hour")
         .when(F.lower(F.col("salary")).contains("year"), "year")
         .when(F.lower(F.col("salary")).contains("month"), "month")
         .when(F.lower(F.col("salary")).contains("week"), "week")
    )
    
    # Normalize salary numbers (handle European vs US number formats)
    def normalize_salary_number(column):
        return (
            F.when(
                column.rlike(r"^\d{1,3}(\.\d{3})+$"),
                F.regexp_replace(column, r"\.", "")
            )
            .when(
                column.rlike(r"^\d{1,3}(,\d{3})+$"),
                F.regexp_replace(column, ",", "")
            )
            .otherwise(column)
            .cast("double")
        )
    
    silver_df = (
        silver_df
        .withColumn("salary_min", normalize_salary_number(F.col("salary_min_raw")))
        .withColumn(
            "salary_max",
            F.when(
                F.col("salary_max_raw") != "",
                normalize_salary_number(F.col("salary_max_raw"))
            )
        )
        .drop("salary_min_raw", "salary_max_raw")
    )
    
    # ========================================
    # 6. WORKPLACE & LOCATION NORMALIZATION
    # ========================================
    # Standardize workplace_type to title case
    silver_df = silver_df.withColumn(
        "workplace_type",
        F.when(F.lower(F.col("workplace_type")) == "remote", "Remote")
         .when(F.lower(F.col("workplace_type")) == "on-site", "On-Site")
         .when(F.lower(F.col("workplace_type")) == "hybrid", "Hybrid")
         .otherwise(F.initcap(F.col("workplace_type")))
    )
    
    # Create boolean flag for remote jobs
    silver_df = silver_df.withColumn(
        "is_remote",
        F.when(F.lower(F.col("workplace_type")) == "remote", True).otherwise(False)
    )
    
    # Standardize country names
    silver_df = silver_df.withColumn(
        "country",
        F.when(F.col("country").isin("U.S.", "USA"), "United States")
         .when(F.col("country") == "U.K.", "United Kingdom")
         .otherwise(F.col("country"))
    )
    
    # Create full location string
    silver_df = silver_df.withColumn(
        "location_full",
        F.when(
            F.col("is_remote") & F.col("country").isNotNull(),
            F.concat(F.lit("Remote - "), F.col("country"))
        )
        .when(
            F.col("is_remote") & F.col("country").isNull(),
            F.lit("Remote")
        )
        .when(
            F.col("city").isNotNull() & F.col("state").isNotNull() & F.col("country").isNotNull(),
            F.concat_ws(", ", F.col("city"), F.col("state"), F.col("country"))
        )
        .when(
            F.col("city").isNotNull() & F.col("country").isNotNull(),
            F.concat_ws(", ", F.col("city"), F.col("country"))
        )
        .when(
            F.col("country").isNotNull(),
            F.col("country")
        )
        .otherwise("Location Not Specified")
    )
    
    # Create shorter location string
    silver_df = silver_df.withColumn(
        "location_short",
        F.when(
            F.col("is_remote"),
            F.lit("Remote")
        )
        .when(
            F.col("city").isNotNull() & F.col("country").isNotNull(),
            F.concat_ws(", ", F.col("city"), F.col("country"))
        )
        .when(
            F.col("country").isNotNull(),
            F.col("country")
        )
        .otherwise("Unknown")
    )
    
    # ========================================
    # 7. ROLE ARRAYS
    # ========================================
    silver_df = (
        silver_df
        .withColumn(
            "role_titles",
            F.transform("roles", lambda x: F.trim(x["title"]))
        )
        .withColumn(
            "role_slugs",
            F.transform("roles", lambda x: F.trim(x["slug"]))
        )
    )
    
    # ========================================
    # 8. FINAL COLUMN SELECTION
    # ========================================
    silver_final = silver_df.select(
        # Job identifiers
        "id",
        "title",
        "url",
        "published_at",
        
        # Company info
        "company_name",
        "company_slug",
        "company_website",
        "company_active_jobs_count",
        
        # Job details
        "employment_type",
        "description",
        "description_html",
        
        # Workplace and location (normalized)
        "workplace_type",
        "is_remote",
        "location_full",
        "location_short",
        "country",
        "country_code",
        
        # Salary (normalized)
        "salary",
        "salary_min",
        "salary_max",
        "salary_currency",
        "salary_period",
        
        # Roles
        "role_titles",
        "role_slugs",
        
        # Metadata
        "_source_file",
        "_source",
        "_ingested_at"
    )
    
    return silver_final
