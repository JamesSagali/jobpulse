import dlt

from pyspark.sql.functions import (
    col,
    lower,
    trim,
    concat_ws,
    lit,
    coalesce,
    regexp_replace,
    concat,
    max as spark_max
)


@dlt.table(
    name="jobpulse_gold.job_skill_match",
    comment="Detailed matching of remote jobs against candidate skills",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def job_skill_match():

    # ---------------------------------------------------------
    # Read source tables
    # ---------------------------------------------------------

    jobs = dlt.read("jobpulse_gold.fact_remote_jobs")

    skills = dlt.read("jobpulse_gold.dim_skill")

    aliases = dlt.read("jobpulse_gold.dim_skill_alias")

    # ---------------------------------------------------------
    # Build searchable job text
    # ---------------------------------------------------------

    jobs = (
        jobs
        .select(
            "id",
            "job_title",
            "description"
        )
        .withColumn(
            "job_text",
            lower(
                concat_ws(
                    " ",
                    coalesce(col("job_title"), lit("")),
                    coalesce(col("description"), lit(""))
                )
            )
        )
        .withColumn(
            "normalized_job_text",
            regexp_replace(
                col("job_text"),
                r"[^a-z0-9+#.]+",
                " "
            )
        )
    )

    # ---------------------------------------------------------
    # Normalize skill aliases
    # ---------------------------------------------------------

    aliases = (
        aliases
        .select(
            "skill_key",
            "alias"
        )
        .withColumn(
            "normalized_alias",
            lower(
                trim(
                    regexp_replace(
                        col("alias"),
                        r"[^a-z0-9+#.]+",
                        " "
                    )
                )
            )
        )
        .filter(
            col("normalized_alias").isNotNull()
            & (col("normalized_alias") != "")
        )
        .dropDuplicates(
            ["skill_key", "normalized_alias"]
        )
    )

    # ---------------------------------------------------------
    # Match jobs against skill aliases
    #
    # Word-boundary style matching is achieved by padding the
    # normalized job text with spaces. This helps prevent
    # partial matches such as:
    #
    # "sql" matching "mysql"
    #
    # ---------------------------------------------------------

    jobs = jobs.withColumn(
        "search_text",
        concat(
            lit(" "),
            trim(col("normalized_job_text")),
            lit(" ")
        )
    )

    matched = (
        jobs
        .crossJoin(aliases)
        .withColumn(
            "skill_matched",
            col("search_text").contains(
                concat(
                    lit(" "),
                    col("normalized_alias"),
                    lit(" ")
                )
            )
        )
    )

    # ---------------------------------------------------------
    # Collapse multiple aliases into one result per
    # job × canonical skill.
    #
    # If any alias matches, the canonical skill matches.
    # ---------------------------------------------------------

    skill_matches = (
        matched
        .groupBy(
            "id",
            "skill_key"
        )
        .agg(
            spark_max(
                col("skill_matched").cast("int")
            )
            .cast("boolean")
            .alias("skill_matched")
        )
    )

    # ---------------------------------------------------------
    # Add canonical skill information
    # ---------------------------------------------------------

    result = (
        skill_matches
        .join(
            skills.select(
                "skill_key",
                "skill_name",
                "skill_category"
            ),
            on="skill_key",
            how="inner"
        )
        .select(
            "id",
            "skill_key",
            "skill_name",
            "skill_category",
            "skill_matched"
        )
    )

    return result