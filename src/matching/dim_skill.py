import dlt

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType
)


@dlt.table(
    name="jobpulse_gold.dim_skill",
    comment="Canonical skills used for job matching",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def dim_skill():

    skills = [

        # ---------------------------------------------------------
        # Analytics & Business Intelligence
        # ---------------------------------------------------------

        (1, "SQL", "Analytics"),
        (2, "Excel", "Analytics"),
        (3, "Power BI", "Business Intelligence"),
        (4, "Tableau", "Business Intelligence"),
        (5, "Looker", "Business Intelligence"),
        (6, "DAX", "Business Intelligence"),
        (7, "Power Query", "Analytics"),
        (8, "Data Visualization", "Analytics"),
        (9, "Data Analysis", "Analytics"),
        (10, "Business Intelligence", "Business Intelligence"),
        (11, "Data Modeling", "Analytics"),
        (12, "Data Cleaning", "Analytics"),
        (13, "Statistics", "Analytics"),

        # ---------------------------------------------------------
        # Programming & Development
        # ---------------------------------------------------------

        (14, "Python", "Programming"),
        (15, "R", "Programming"),
        (16, "Pandas", "Programming"),
        (17, "NumPy", "Programming"),
        (18, "Jupyter", "Programming"),
        (19, "Git", "Development"),
        (20, "GitHub", "Development"),

        # ---------------------------------------------------------
        # Data Engineering
        # ---------------------------------------------------------

        (21, "Databricks", "Data Engineering"),
        (22, "Apache Spark", "Data Engineering"),
        (23, "PySpark", "Data Engineering"),
        (24, "Airflow", "Data Engineering"),
        (25, "Kafka", "Data Engineering"),
        (26, "dbt", "Data Engineering"),
        (27, "ETL", "Data Engineering"),
        (28, "ELT", "Data Engineering"),
        (29, "Data Pipelines", "Data Engineering"),
        (30, "Data Warehousing", "Data Engineering"),
        (31, "Data Lake", "Data Engineering"),
        (32, "Lakehouse", "Data Engineering"),
        (33, "Delta Lake", "Data Engineering"),
        (34, "Docker", "Data Engineering"),

        # ---------------------------------------------------------
        # Databases & Data Platforms
        # ---------------------------------------------------------

        (35, "MySQL", "Database"),
        (36, "PostgreSQL", "Database"),
        (37, "SQL Server", "Database"),
        (38, "Snowflake", "Data Platform"),
        (39, "BigQuery", "Data Platform"),

        # ---------------------------------------------------------
        # IT & Technical Support
        # ---------------------------------------------------------

        (40, "Zendesk", "Support"),
        (41, "ServiceNow", "Support"),
        (42, "Genesys Cloud", "Support"),
        (43, "Jira", "Support"),
        (44, "Technical Support", "Support"),
        (45, "Customer Support", "Support"),
        (46, "Customer Service", "Support"),
        (47, "Application Support", "IT Support"),
        (48, "Production Support", "IT Support"),
        (49, "Troubleshooting", "IT Support"),
        (50, "Incident Management", "IT Support"),
        (51, "Root Cause Analysis", "IT Support"),
        (52, "SLA", "IT Support"),
        (53, "Escalation Management", "IT Support"),
        (54, "ITIL", "IT Support"),
        (55, "IT Operations", "IT Support"),
        (56, "IT Security", "IT Support"),
        (57, "IT Architecture", "IT Support"),
        (58, "IT Governance", "IT Support"),
        (59, "IT Compliance", "IT Support"),
        (60, "IT Risk Management", "IT Support"),
        (61, "Customer Support Tools", "IT Support"),
        (62, "IT Help Desk", "IT Support"),
        (63, "IT Asset Management", "IT Support"),
        (64, "IT Configuration Management", "IT Support"),
        (65, "Support Tools", "IT Support"),
        (66, "IT Service Management", "IT Support"),
        (67, "IT Service Desk", "IT Support"),
        (68, "IT Service Level Agreement", "IT Support"),
        (69, "IT Service Management Practices", "IT Support")
    ]
  

    schema = StructType([
        StructField("skill_key", IntegerType(), False),
        StructField("skill_name", StringType(), False),
        StructField("skill_category", StringType(), False)
    ])

    return spark.createDataFrame(skills, schema)