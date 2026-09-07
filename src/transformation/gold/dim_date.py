import dlt
from pyspark.sql.functions import col, to_date, dayofweek, month, year, dayofmonth, date_format, quarter, row_number
from pyspark.sql.window import Window

@dlt.table(
    name="jobpulse_gold.dim_date",
    comment="Date dimension table with calendar attributes",
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"}
)
def dim_date():
    """
    Create date dimension with surrogate key and calendar attributes.
    Includes day, month, year, day of week, and formatted date strings.
    """
    return (
        dlt.read("workspace.jobpulse_silver.silver_jobs")
        .select(to_date(col("published_at")).alias("full_date"))
        .distinct()
        .withColumn("day_of_month", dayofmonth(col("full_date")))
        .withColumn("month", month(col("full_date")))
        .withColumn("quarter", quarter(col("full_date")))
        .withColumn("year", year(col("full_date")))
        .withColumn("day_of_week", dayofweek(col("full_date")))
        .withColumn("month_name", date_format(col("full_date"), "MMMM"))
        .withColumn("day_name", date_format(col("full_date"), "EEEE"))
        .withColumn("row_num", row_number().over(Window.orderBy("full_date")))
        .withColumn("date_key", col("row_num"))
        .select("date_key", "full_date", "day_of_month", "month", "quarter", "year", "day_of_week", "month_name", "day_name")
    )