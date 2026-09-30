from pyspark.sql import functions as F

base = (
    df_stores
    .filter(F.col("store_name").isNull() & F.col("store_name").like("Old%") & (F.lower(F.trim("store_name")) == "old town"))
)

stores_stats = (
    base
    .groupBy(
        F.month("opened_date").alias("opened_month"),
        F.when(F.col("sqft") >= 12209, "high").when(F.col("sqft") >= 4571, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.max("opened_date").alias("last_opened_date"))
)

result = stores_stats.select("opened_month", "sqft_bucket", "last_opened_date")
