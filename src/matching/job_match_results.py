import dlt

from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    round,
    when,
    lit,
    max as spark_max,
    row_number
)
from pyspark.sql.window import Window


@dlt.table(
    name="jobpulse_gold.job_match_results",
    comment="Job-level candidate matching results combining role and role-relevant skill fit",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def job_match_results():

    # =========================================================
    # READ TABLES
    # =========================================================

    jobs = dlt.read(
        "jobpulse_gold.fact_remote_jobs"
    )

    skill_matches = dlt.read(
        "jobpulse_gold.job_skill_match"
    )

    bridge = dlt.read(
        "jobpulse_gold.bridge_job_role"
    )

    roles = dlt.read(
        "jobpulse_gold.dim_role"
    )

    candidate_roles = dlt.read(
        "jobpulse_gold.candidate_role_match"
    )


    # =========================================================
    # GET JOB ROLES
    # =========================================================

    job_roles = (
        bridge
        .join(
            roles,
            "role_key",
            "inner"
        )
        .select(
            bridge.id,
            roles.role_title
        )
        .distinct()
    )


    # =========================================================
    # MATCH JOB ROLES TO CANDIDATE TARGET ROLES
    # =========================================================

    role_matches = (
        job_roles
        .join(
            candidate_roles.select(
                "candidate_role",
                "candidate_role_weight"
            ).distinct(),
            job_roles.role_title
            == candidate_roles.candidate_role,
            "inner"
        )
        .select(
            job_roles.id,
            candidate_roles.candidate_role,
            candidate_roles.candidate_role_weight
        )
        .distinct()
    )


    # =========================================================
    # CALCULATE SKILL MATCH FOR EACH JOB + TARGET ROLE
    #
    # Example:
    #
    # Data Analyst has 18 relevant skills.
    # Job matches 14 of them.
    #
    # Skill score = 14 / 18 * 100 = 77.78
    # =========================================================

    role_skill_requirements = (
        candidate_roles
        .select(
            "candidate_role",
            "skill_name"
        )
        .distinct()
    )


    # Match the job's detected skills against the skills
    # relevant to each candidate target role.

    role_skill_matches = (
        role_matches.alias("rm")
        .join(
            role_skill_requirements.alias("rsr"),
            "candidate_role",
            "inner"
        )
        .join(
            skill_matches.alias("sm"),
            (
                (col("rm.id") == col("sm.id"))
                &
                (
                    col("rsr.skill_name")
                    == col("sm.skill_name")
                )
            ),
            "left"
        )
        .select(
            col("rm.id").alias("id"),
            col("candidate_role"),
            col("rm.candidate_role_weight").alias("candidate_role_weight"),
            col("rsr.skill_name").alias("skill_name"),
            when(
                col("sm.skill_matched") == True,
                1
            ).otherwise(
                0
            ).alias("skill_matched")
        )
    )


    # =========================================================
    # SUM SKILLS FOR EACH JOB + ROLE
    # =========================================================

    role_skill_summary = (
        role_skill_matches
        .groupBy(
            "id",
            "candidate_role",
            "candidate_role_weight"
        )
        .agg(
            count(
                "skill_name"
            ).alias(
                "relevant_skills"
            ),

            spark_sum(
                "skill_matched"
            ).alias(
                "skills_matched"
            )
        )
        .withColumn(
            "skill_match_score",
            round(
                when(
                    col("relevant_skills") > 0,
                    (
                        col("skills_matched")
                        /
                        col("relevant_skills")
                    ) * 100
                ).otherwise(
                    0
                ),
                2
            )
        )
    )


    # =========================================================
    # SELECT BEST ROLE MATCH FOR EACH JOB
    #
    # We prioritize:
    # 1. Highest skill match score
    # 2. Highest candidate role priority
    # =========================================================

    role_window = Window.partitionBy(
        "id"
    ).orderBy(
        col("skill_match_score").desc(),
        col("candidate_role_weight").desc()
    )


    best_role_match = (
        role_skill_summary
        .withColumn(
            "rn",
            row_number().over(role_window)
        )
        .filter(
            col("rn") == 1
        )
        .select(
            "id",
            col("candidate_role").alias(
                "matched_candidate_role"
            ),
            "candidate_role_weight",
            "relevant_skills",
            "skills_matched",
            "skill_match_score"
        )
    )


    # =========================================================
    # JOB-LEVEL RESULT
    # =========================================================

    result = (
        jobs
        .select(
            "id",
            "job_title",
            "job_url",
            "company_key",
            "location_key",
            "employment_type_key",
            "salary_key",
            "salary_min",
            "salary_max",
            "salary_mid",
            "has_salary_info",
            "published_at"
        )
        .join(
            best_role_match,
            "id",
            "left"
        )
    )


    # =========================================================
    # HANDLE JOBS WITH NO TARGET ROLE MATCH
    # =========================================================

    result = result.fillna({
        "candidate_role_weight": 0.0,
        "relevant_skills": 0,
        "skills_matched": 0,
        "skill_match_score": 0.0
    })


    # =========================================================
    # ROLE MATCH SCORE
    #
    # If the job matches one of the candidate's target roles:
    #     100
    #
    # Otherwise:
    #     0
    # =========================================================

    result = result.withColumn(
        "role_match_score",
        when(
            col("matched_candidate_role").isNotNull(),
            lit(100.0)
        ).otherwise(
            lit(0.0)
        )
    )


    # =========================================================
    # OVERALL MATCH SCORE
    #
    # 40% role fit
    # 60% skill fit
    # =========================================================

    result = result.withColumn(
        "overall_match_score",
        round(
            (
                col("role_match_score")
                * lit(0.40)
            )
            +
            (
                col("skill_match_score")
                * lit(0.60)
            ),
            2
        )
    )


    # =========================================================
    # MATCH CATEGORY
    # =========================================================

    result = result.withColumn(
        "match_category",
        when(
            col("overall_match_score") >= 80,
            "Excellent Match"
        )
        .when(
            col("overall_match_score") >= 65,
            "Strong Match"
        )
        .when(
            col("overall_match_score") >= 50,
            "Moderate Match"
        )
        .when(
            col("overall_match_score") >= 30,
            "Weak Match"
        )
        .otherwise(
            "Poor Match"
        )
    )


    return result