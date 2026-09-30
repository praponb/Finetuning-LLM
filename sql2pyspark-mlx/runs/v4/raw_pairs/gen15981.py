from pyspark.sql import functions as F

base = df_store_sales.filter(F.col("store_id").isin([1, 3, 5]) & (F.col("revenue") > 2500))

revenue_bucket_summary = (
    base
    .groupBy(
        F.when(F.col("revenue") >= 3200, "high").when(F.col("revenue") >= 1300, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(
        F.count("revenue").alias("revenue_count"),
        F.count("units").alias("units_count"),
    )
)

result = (
    revenue_bucket_summary
    .select("revenue_bucket", "units_count")
    .orderBy("revenue_bucket", F.col("units_count").desc())
)
