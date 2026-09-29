from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        "product_id",
        "store_id",
        F.when(F.col("revenue") >= 3100, "high").when(F.col("revenue") >= 1000, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(
        F.sum("units").alias("total_units"),
        F.min("revenue").alias("min_revenue"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
    )
    .filter(F.col("min_revenue") >= 1900)
    .orderBy("total_units", "product_id", "store_id", "revenue_bucket")
    .limit(5)
)
