import dlt

from pyspark.sql.functions import (
    col,
    lower,
    trim,
    concat_ws,
    lit
)


@dlt.table(
    name="jobpulse_gold.bridge_job_role",
    comment="Bridge table for many-to-many relationship between jobs and roles",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bridge_job_role():

    silver = (
        dlt.read(
            "workspace.jobpulse_silver.silver_jobs"
        )
        .filter(
            col("is_remote") == True
        )
    )

    aliases = (
        dlt.read(
            "jobpulse_gold.dim_role_alias"
        )
        .select(
            "canonical_role",
            "role_alias"
        )
        .filter(
            col("role_alias").isNotNull()
        )
        .dropDuplicates(
            ["canonical_role", "role_alias"]
        )
    )

    dim_role = dlt.read(
        "jobpulse_gold.dim_role"
    )

    # ---------------------------------------------------------
    # Convert role_titles array into searchable text
    # ---------------------------------------------------------

    jobs = (
        silver
        .select(
            "id",
            "role_titles"
        )
        .withColumn(
            "role_text",
            lower(
                concat_ws(
                    " ",
                    col("role_titles")
                )
            )
        )
    )

    # ---------------------------------------------------------
    # Match job roles against aliases
    # ---------------------------------------------------------

    matched_roles = (
        jobs
        .crossJoin(aliases)
        .filter(
            col("role_text").contains(
                lower(
                    trim(
                        col("role_alias")
                    )
                )
            )
        )
        .select(
            col("id"),
            col("canonical_role")
        )
        .distinct()
    )

    # ---------------------------------------------------------
    # Resolve canonical role to dim_role
    # ---------------------------------------------------------

    result = (
        matched_roles
        .join(
            dim_role,
            lower(trim(matched_roles.canonical_role))
            == lower(trim(dim_role.role_title)),
            "inner"
        )
        .select(
            matched_roles.id,
            dim_role.role_key
        )
        .distinct()
    )

    return result