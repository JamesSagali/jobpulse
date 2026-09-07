import dlt
from pyspark.sql.functions import col, when, lit, row_number
from pyspark.sql.window import Window

@dlt.table(
    name="jobpulse_gold.dim_salary",
    comment="Salary band dimension table",
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"}
)
def dim_salary():
    """
    Create salary band dimension with surrogate key.
    Creates salary ranges based on midpoint values.
    """
    # Define salary bands
    salary_bands = [
        (1, "Not Specified", None, None),
        (2, "0-50K", 0, 50000),
        (3, "50K-75K", 50000, 75000),
        (4, "75K-100K", 75000, 100000),
        (5, "100K-125K", 100000, 125000),
        (6, "125K-150K", 125000, 150000),
        (7, "150K-175K", 150000, 175000),
        (8, "175K-200K", 175000, 200000),
        (9, "200K-250K", 200000, 250000),
        (10, "250K-300K", 250000, 300000),
        (11, "300K-400K", 300000, 400000),
        (12, "400K+", 400000, None)
    ]
    
    return (
        spark.createDataFrame(
            salary_bands,
            ["salary_key", "salary_band", "min_salary", "max_salary"]
        )
        .withColumn("salary_currency", lit(None).cast("string"))
        .withColumn("salary_period", lit(None).cast("string"))
    )