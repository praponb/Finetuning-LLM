from pyspark.sql import functions as F

filtered_stores = df_stores.filter((F.col("sqft") > 10357) | (F.col("sqft") <= 10727))

stores_stats = (
    filtered_stores
    .groupBy(
        F.when(F.col("sqft") >= 16014, "high").when(F.col("sqft") >= 7121, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.max("opened_date").alias("last_opened_date"),
        F.count(F.when(F.col("region") == "East", 1)).alias("east_count"),
    )
)

result = (
    stores_stats
    .select("sqft_bucket", "east_count")
    .orderBy(F.col("east_count").desc(), "sqft_bucket")
    .limit(10)
)
