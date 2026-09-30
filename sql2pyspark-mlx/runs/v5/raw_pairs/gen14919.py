from pyspark.sql import functions as F

filtered_stores = (
    df_stores
    .filter((F.col("opened_date") < "2024-09-01") & ((F.col("opened_date") < "2025-10-14") | (F.col("sqft") < 5390)))
    .withColumn("city_title", F.initcap(F.trim("city")))
)

summary = (
    filtered_stores
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.sum(F.when(F.col("city") == "Singapore", 1).otherwise(0)).alias("singapore_count"),
    )
)

result = (
    summary
    .filter(F.col("singapore_count") >= 73)
    .orderBy("min_opened_date")
)
