from pyspark.sql import functions as F

recent_store_sales = df_store_sales.filter(F.col("revenue") > 3600)

store_sales_stats = (
    recent_store_sales
    .groupBy("store_id")
    .agg(
        F.min("revenue").alias("min_revenue"),
        F.round(F.avg("units"), 2).alias("avg_units"),
    )
)

result = (
    store_sales_stats
    .filter(F.col("min_revenue") < 1700)
    .select("store_id", "min_revenue", "avg_units")
    .orderBy(F.col("min_revenue").desc())
)
