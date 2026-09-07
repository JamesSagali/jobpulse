import dlt

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)


@dlt.table(
    name="jobpulse_gold.dim_role_alias",
    comment="Alternative job titles mapped to canonical job titles",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def dim_role_alias():

    aliases = [

        # ---------------------------------------------------------
        # Data Analyst
        # ---------------------------------------------------------

        ("Data Analyst", "data analyst"),
        ("Data Analyst", "data analytics analyst"),
        ("Data Analyst", "data analysis analyst"),
        ("Data Analyst", "reporting analyst"),
        ("Data Analyst", "business data analyst"),
        ("Data Analyst", "junior data analyst"),
        ("Data Analyst", "senior data analyst"),
        ("Data Analyst", "data analyst specialist"),

        # ---------------------------------------------------------
        # Data Engineer
        # ---------------------------------------------------------

        ("Data Engineer", "data engineer"),
        ("Data Engineer", "data engineering specialist"),
        ("Data Engineer", "data engineering analyst"),
        ("Data Engineer", "junior data engineer"),
        ("Data Engineer", "senior data engineer"),
        ("Data Engineer", "data platform engineer"),
        ("Data Engineer", "etl developer"),
        ("Data Engineer", "etl engineer"),

        # ---------------------------------------------------------
        # BI Analyst
        # ---------------------------------------------------------

        ("BI Analyst", "bi analyst"),
        ("BI Analyst", "business intelligence analyst"),
        ("BI Analyst", "business intelligence specialist"),
        ("BI Analyst", "business intelligence developer"),
        ("BI Analyst", "bi developer"),
        ("BI Analyst", "business analytics analyst"),

        # ---------------------------------------------------------
        # Analytics Engineer
        # ---------------------------------------------------------

        ("Analytics Engineer", "analytics engineer"),
        ("Analytics Engineer", "analytics engineering"),
        ("Analytics Engineer", "data analytics engineer"),
        ("Analytics Engineer", "business analytics engineer"),

        # ---------------------------------------------------------
        # Technical Support Analyst
        # ---------------------------------------------------------

        ("Technical Support Analyst", "technical support analyst"),
        ("Technical Support Analyst", "technical support engineer"),
        ("Technical Support Analyst", "technical support specialist"),
        ("Technical Support Analyst", "technical support associate"),
        ("Technical Support Analyst", "technical support representative"),
        ("Technical Support Analyst", "technical support consultant"),
        ("Technical Support Analyst", "technical support technician"),
        ("Technical Support Analyst", "support engineer"),
        ("Technical Support Analyst", "support analyst"),
        ("Technical Support Analyst", "support specialist"),
        ("Technical Support Analyst", "technical support"),
        ("Technical Support Analyst", "it support analyst"),
        ("Technical Support Analyst", "it support engineer"),
        ("Technical Support Analyst", "it support specialist"),
        ("Technical Support Analyst", "technical services analyst"),

        # ---------------------------------------------------------
        # Customer Support Specialist
        # ---------------------------------------------------------

        ("Customer Support Specialist", "customer support specialist"),
        ("Customer Support Specialist", "customer support representative"),
        ("Customer Support Specialist", "customer support associate"),
        ("Customer Support Specialist", "customer support agent"),
        ("Customer Support Specialist", "customer care specialist"),
        ("Customer Support Specialist", "customer care representative"),
        ("Customer Support Specialist", "customer experience specialist"),
        ("Customer Support Specialist", "customer experience representative"),
        ("Customer Support Specialist", "client support specialist"),
        ("Customer Support Specialist", "client service specialist"),
        ("Customer Support Specialist", "customer success specialist")
    ]

    schema = StructType([
        StructField(
            "canonical_role",
            StringType(),
            False
        ),
        StructField(
            "role_alias",
            StringType(),
            False
        )
    ])

    return spark.createDataFrame(
        aliases,
        schema
    )