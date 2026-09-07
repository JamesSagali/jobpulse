import dlt
from pyspark.sql.functions import col, when, row_number
from pyspark.sql.window import Window

@dlt.table(
    name="jobpulse_gold.dim_location",
    comment="Location dimension table with region classification",
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"}
)
def dim_location():
    """
    Create location dimension with surrogate key and region.
    Classifies locations into regions based on country/location patterns.
    """
    return (
        dlt.read("workspace.jobpulse_silver.silver_jobs")
        .select("location_full", "country", "country_code")
        .distinct()
        .withColumn(
            "region",
            when(col("location_full").rlike("(?i)(US|USA|United States|America|CA|NY|TX|FL|IL|WA|MA)"), "North America")
            .when(col("location_full").rlike("(?i)(UK|United Kingdom|London|Europe|Germany|France|Spain|Italy|Netherlands|Belgium|Poland|Sweden)"), "Europe")
            .when(col("location_full").rlike("(?i)(India|Singapore|Japan|China|Asia|Hong Kong|Taiwan|Malaysia|Indonesia)"), "Asia")
            .when(col("location_full").rlike("(?i)(Australia|Sydney|Melbourne|New Zealand)"), "Australia/Oceania")
            .when(col("location_full").rlike("(?i)(Brazil|Mexico|Argentina|Latin America|South America)"), "Latin America")
            .when(col("location_full").rlike("(?i)(Remote|Worldwide|Global|Anywhere)"), "Remote/Global")
            .otherwise("Other")
        )
        .withColumn("row_num", row_number().over(Window.orderBy("location_full")))
        .withColumn("location_key", col("row_num"))
        .select("location_key", "location_full", "country", "country_code", "region")
    )