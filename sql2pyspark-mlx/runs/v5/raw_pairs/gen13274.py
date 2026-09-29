from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.max("revenue").alias("max_revenue"),
        F.avg("revenue").alias("avg_revenue"),
        F.sum("revenue").alias("total_revenue"),
    )
)
