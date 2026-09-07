import dlt

from pyspark.sql.functions import (
    col,
    explode,
    arrays_zip,
    trim,
    upper,
    when,
    row_number,
    lit
)
from pyspark.sql.window import Window


@dlt.table(
    name="jobpulse_gold.dim_role",
    comment="Role dimension table with categorization and canonical candidate roles",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def dim_role():

    # ---------------------------------------------------------
    # Extract roles from source data
    # ---------------------------------------------------------

    source_roles = (
        dlt.read(
            "workspace.jobpulse_silver.silver_jobs"
        )
        .select(
            explode(
                arrays_zip(
                    col("role_titles"),
                    col("role_slugs")
                )
            ).alias("zipped")
        )
        .select(
            trim(
                col("zipped.role_titles")
            ).alias("role_title"),
            trim(
                col("zipped.role_slugs")
            ).alias("role_slug")
        )
        .filter(
            col("role_title").isNotNull()
            & (col("role_title") != "")
        )
        .distinct()
    )

    # ---------------------------------------------------------
    # Add canonical candidate roles that may not exist
    # exactly in source data
    # ---------------------------------------------------------

    canonical_roles = (
        spark.createDataFrame(
            [
                ("Data Analyst", "data-analyst"),
                ("Data Engineer", "data-engineer"),
                ("BI Analyst", "bi-analyst"),
                ("Analytics Engineer", "analytics-engineer"),
                ("Technical Support Analyst", "technical-support-analyst"),
                ("Customer Support Specialist", "customer-support-specialist")
            ],
            ["role_title", "role_slug"]
        )
    )

    roles = (
        source_roles
        .unionByName(
            canonical_roles
        )
        .distinct()
    )

    # ---------------------------------------------------------
    # Categorize roles
    # ---------------------------------------------------------

    roles = (
        roles
        .withColumn(
            "role_title_upper",
            upper(col("role_title"))
        )
        .withColumn(
            "role_category",
            when(
                col("role_title_upper").rlike(
                    "(DATA SCIENTIST|DATA SCIENCE|ML |MACHINE LEARNING)"
                ),
                "Data Science & ML"
            )
            .when(
                col("role_title_upper").rlike(
                    "(DATA ENGINEER|ETL|PIPELINE)"
                ),
                "Data Engineering"
            )
            .when(
                col("role_title_upper").rlike(
                    "(ANALYST|BUSINESS ANALYST|DATA ANALYST)"
                ),
                "Analytics"
            )
            .when(
                col("role_title_upper").rlike(
                    "(SOFTWARE ENGINEER|DEVELOPER|PROGRAMMER|BACKEND|FRONTEND|FULLSTACK|FULL STACK)"
                ),
                "Software Engineering"
            )
            .when(
                col("role_title_upper").rlike(
                    "(DEVOPS|SRE|SITE RELIABILITY|CLOUD|INFRASTRUCTURE)"
                ),
                "DevOps & Infrastructure"
            )
            .when(
                col("role_title_upper").rlike(
                    "(MANAGER|DIRECTOR|HEAD OF|LEAD|VP|CHIEF)"
                ),
                "Management & Leadership"
            )
            .when(
                col("role_title_upper").rlike(
                    "(ARCHITECT|PRINCIPAL|STAFF)"
                ),
                "Architecture & Principal"
            )
            .when(
                col("role_title_upper").rlike(
                    "(PRODUCT|PM)"
                ),
                "Product"
            )
            .when(
                col("role_title_upper").rlike(
                    "(SECURITY|CYBER)"
                ),
                "Security"
            )
            .otherwise(
                "Other"
            )
        )
    )

    # ---------------------------------------------------------
    # Generate surrogate key
    # ---------------------------------------------------------

    window = Window.orderBy(
        "role_title",
        "role_slug"
    )

    return (
        roles
        .withColumn(
            "row_num",
            row_number().over(window)
        )
        .withColumn(
            "role_key",
            col("row_num")
        )
        .select(
            "role_key",
            "role_title",
            "role_slug",
            "role_category"
        )
    )