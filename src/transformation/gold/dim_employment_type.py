import dlt
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

@dlt.table(
    name="jobpulse_gold.dim_employment_type",
    comment="Employment type dimension table",
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"}
)
def dim_employment_type():
    """
    Create employment type dimension with surrogate key.
    Represents Full-time, Part-time, Contract, and other employment types.
    """
    return (
        dlt.read("workspace.jobpulse_silver.silver_jobs")
        .select("employment_type")
        .distinct()
        .withColumn("row_num", row_number().over(Window.orderBy("employment_type")))
        .withColumn("employment_type_key", col("row_num"))
        .select("employment_type_key", "employment_type")
    )