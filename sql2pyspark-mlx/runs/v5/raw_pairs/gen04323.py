from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        F.min("revenue").alias("min_revenue"),
    )
    .filter(F.col("min_revenue") >= 2100)
)
