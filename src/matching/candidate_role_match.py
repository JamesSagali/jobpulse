import dlt

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)


@dlt.table(
    name="jobpulse_gold.candidate_role_match",
    comment="Candidate target roles, priorities, and relevant skills for job matching",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def candidate_role_match():

    role_skills = [

        # =========================================================
        # DATA ANALYST
        # =========================================================
        ("Data Analyst", 0.25, "SQL"),
        ("Data Analyst", 0.25, "Excel"),
        ("Data Analyst", 0.25, "Power BI"),
        ("Data Analyst", 0.25, "Tableau"),
        ("Data Analyst", 0.25, "Looker"),
        ("Data Analyst", 0.25, "DAX"),
        ("Data Analyst", 0.25, "Power Query"),
        ("Data Analyst", 0.25, "Data Visualization"),
        ("Data Analyst", 0.25, "Data Analysis"),
        ("Data Analyst", 0.25, "Business Intelligence"),
        ("Data Analyst", 0.25, "Data Modeling"),
        ("Data Analyst", 0.25, "Data Cleaning"),
        ("Data Analyst", 0.25, "Statistics"),
        ("Data Analyst", 0.25, "Python"),
        ("Data Analyst", 0.25, "Pandas"),
        ("Data Analyst", 0.25, "NumPy"),
        ("Data Analyst", 0.25, "Jupyter"),
        ("Data Analyst", 0.25, "Git"),
        ("Data Analyst", 0.25, "GitHub"),

        # =========================================================
        # DATA ENGINEER
        # =========================================================
        ("Data Engineer", 0.25, "SQL"),
        ("Data Engineer", 0.25, "Python"),
        ("Data Engineer", 0.25, "Pandas"),
        ("Data Engineer", 0.25, "NumPy"),
        ("Data Engineer", 0.25, "Git"),
        ("Data Engineer", 0.25, "GitHub"),
        ("Data Engineer", 0.25, "Databricks"),
        ("Data Engineer", 0.25, "Apache Spark"),
        ("Data Engineer", 0.25, "PySpark"),
        ("Data Engineer", 0.25, "Airflow"),
        ("Data Engineer", 0.25, "Kafka"),
        ("Data Engineer", 0.25, "dbt"),
        ("Data Engineer", 0.25, "ETL"),
        ("Data Engineer", 0.25, "ELT"),
        ("Data Engineer", 0.25, "Data Pipelines"),
        ("Data Engineer", 0.25, "Data Warehousing"),
        ("Data Engineer", 0.25, "Data Lake"),
        ("Data Engineer", 0.25, "Lakehouse"),
        ("Data Engineer", 0.25, "Delta Lake"),
        ("Data Engineer", 0.25, "Docker"),
        ("Data Engineer", 0.25, "MySQL"),
        ("Data Engineer", 0.25, "PostgreSQL"),
        ("Data Engineer", 0.25, "SQL Server"),
        ("Data Engineer", 0.25, "Snowflake"),
        ("Data Engineer", 0.25, "BigQuery"),

        # =========================================================
        # BI ANALYST
        # =========================================================
        ("BI Analyst", 0.15, "SQL"),
        ("BI Analyst", 0.15, "Excel"),
        ("BI Analyst", 0.15, "Power BI"),
        ("BI Analyst", 0.15, "Tableau"),
        ("BI Analyst", 0.15, "Looker"),
        ("BI Analyst", 0.15, "DAX"),
        ("BI Analyst", 0.15, "Power Query"),
        ("BI Analyst", 0.15, "Data Visualization"),
        ("BI Analyst", 0.15, "Data Analysis"),
        ("BI Analyst", 0.15, "Business Intelligence"),
        ("BI Analyst", 0.15, "Data Modeling"),
        ("BI Analyst", 0.15, "Data Cleaning"),
        ("BI Analyst", 0.15, "Statistics"),
        ("BI Analyst", 0.15, "Python"),
        ("BI Analyst", 0.15, "Git"),
        ("BI Analyst", 0.15, "GitHub"),

        # =========================================================
        # ANALYTICS ENGINEER
        # =========================================================
        ("Analytics Engineer", 0.15, "SQL"),
        ("Analytics Engineer", 0.15, "Python"),
        ("Analytics Engineer", 0.15, "Git"),
        ("Analytics Engineer", 0.15, "GitHub"),
        ("Analytics Engineer", 0.15, "dbt"),
        ("Analytics Engineer", 0.15, "ETL"),
        ("Analytics Engineer", 0.15, "ELT"),
        ("Analytics Engineer", 0.15, "Data Pipelines"),
        ("Analytics Engineer", 0.15, "Data Warehousing"),
        ("Analytics Engineer", 0.15, "Data Modeling"),
        ("Analytics Engineer", 0.15, "Data Cleaning"),
        ("Analytics Engineer", 0.15, "Data Analysis"),
        ("Analytics Engineer", 0.15, "Business Intelligence"),
        ("Analytics Engineer", 0.15, "Data Visualization"),
        ("Analytics Engineer", 0.15, "Snowflake"),
        ("Analytics Engineer", 0.15, "BigQuery"),
        ("Analytics Engineer", 0.15, "Databricks"),

        # =========================================================
        # TECHNICAL SUPPORT ANALYST
        # =========================================================
        ("Technical Support Analyst", 0.10, "SQL"),
        ("Technical Support Analyst", 0.10, "Excel"),
        ("Technical Support Analyst", 0.10, "Python"),
        ("Technical Support Analyst", 0.10, "MySQL"),
        ("Technical Support Analyst", 0.10, "PostgreSQL"),
        ("Technical Support Analyst", 0.10, "Zendesk"),
        ("Technical Support Analyst", 0.10, "ServiceNow"),
        ("Technical Support Analyst", 0.10, "Genesys Cloud"),
        ("Technical Support Analyst", 0.10, "Jira"),
        ("Technical Support Analyst", 0.10, "Technical Support"),
        ("Technical Support Analyst", 0.10, "Application Support"),
        ("Technical Support Analyst", 0.10, "Production Support"),
        ("Technical Support Analyst", 0.10, "Troubleshooting"),
        ("Technical Support Analyst", 0.10, "Incident Management"),
        ("Technical Support Analyst", 0.10, "Root Cause Analysis"),
        ("Technical Support Analyst", 0.10, "SLA"),
        ("Technical Support Analyst", 0.10, "Escalation Management"),
        ("Technical Support Analyst", 0.10, "ITIL"),
        ("Technical Support Analyst", 0.10, "IT Operations"),
        ("Technical Support Analyst", 0.10, "Customer Support Tools"),
        ("Technical Support Analyst", 0.10, "IT Help Desk"),
        ("Technical Support Analyst", 0.10, "IT Configuration Management"),
        ("Technical Support Analyst", 0.10, "Support Tools"),
        ("Technical Support Analyst", 0.10, "IT Service Management"),
        ("Technical Support Analyst", 0.10, "IT Service Desk"),
        ("Technical Support Analyst", 0.10, "IT Service Level Agreement"),

        # =========================================================
        # CUSTOMER SUPPORT SPECIALIST
        # =========================================================
        ("Customer Support Specialist", 0.10, "Excel"),
        ("Customer Support Specialist", 0.10, "Zendesk"),
        ("Customer Support Specialist", 0.10, "ServiceNow"),
        ("Customer Support Specialist", 0.10, "Genesys Cloud"),
        ("Customer Support Specialist", 0.10, "Jira"),
        ("Customer Support Specialist", 0.10, "Technical Support"),
        ("Customer Support Specialist", 0.10, "Customer Support"),
        ("Customer Support Specialist", 0.10, "Customer Service"),
        ("Customer Support Specialist", 0.10, "Application Support"),
        ("Customer Support Specialist", 0.10, "Troubleshooting"),
        ("Customer Support Specialist", 0.10, "Incident Management"),
        ("Customer Support Specialist", 0.10, "Root Cause Analysis"),
        ("Customer Support Specialist", 0.10, "SLA"),
        ("Customer Support Specialist", 0.10, "Escalation Management"),
        ("Customer Support Specialist", 0.10, "Customer Support Tools"),
        ("Customer Support Specialist", 0.10, "IT Help Desk"),
        ("Customer Support Specialist", 0.10, "Support Tools"),
        ("Customer Support Specialist", 0.10, "IT Service Desk"),
        ("Customer Support Specialist", 0.10, "IT Service Level Agreement")
    ]

    schema = StructType([
        StructField(
            "candidate_role",
            StringType(),
            False
        ),
        StructField(
            "candidate_role_weight",
            DoubleType(),
            False
        ),
        StructField(
            "skill_name",
            StringType(),
            False
        )
    ])

    return (
        spark.createDataFrame(
            role_skills,
            schema
        )
        .dropDuplicates(
            ["candidate_role", "skill_name"]
        )
    )