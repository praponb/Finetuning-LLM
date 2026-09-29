from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") <= 14335)
    .groupBy("city", F.year("opened_date").alias("opened_year"))
    .agg(
        F.countDistinct("sqft").alias("unique_regions"),
        F.max("opened_date").alias("last_opened_date"),
    )
    .filter(F.col("unique_regions") < 9)
    .select("city", "opened_year", "unique_regions", "last_opened_date")
)
