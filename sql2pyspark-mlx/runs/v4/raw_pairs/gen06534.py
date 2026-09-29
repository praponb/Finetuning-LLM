from pyspark.sql import functions as F

valid_stores = df_stores.filter(F.col("opened_date") >= "2025-07-01")

stores_stats = (
    valid_stores
    .groupBy("city", F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.sum(F.when(F.col("region") == "South", 1).otherwise(0)).alias("south_count"),
    )
)

result = (
    stores_stats
    .select("city", "opened_month", "sqft_range", "south_count")
    .orderBy(F.col("south_count").desc(), "sqft_range")
)
