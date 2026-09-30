from pyspark.sql import functions as F

valid_stores = df_stores.filter(F.col("city") == "London")

stores_stats = (
    valid_stores
    .groupBy(
        F.when(F.col("sqft") >= 17233, "high").when(F.col("sqft") >= 6278, "mid").otherwise("low").alias("sqft_bucket"),
        F.date_format("opened_date", "yyyy-MM").alias("opened_month"),
    )
    .agg(
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
        F.sum("sqft").alias("total_sqft"),
    )
)

result = (
    stores_stats
    .filter(F.col("total_sqft") > 79000)
    .select("sqft_bucket", "opened_month", "london_count", "total_sqft")
)
