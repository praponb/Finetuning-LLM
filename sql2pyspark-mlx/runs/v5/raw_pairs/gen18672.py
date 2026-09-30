from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
        F.count("revenue").alias("revenue_count"),
    )
    .filter(F.col("revenue_count") > 8)
    .select("store_id", "revenue_range", "revenue_count")
    .orderBy(F.col("store_id").desc(), "revenue_range")
)
