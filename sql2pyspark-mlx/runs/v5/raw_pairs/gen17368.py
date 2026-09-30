from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") < 2100)
    .groupBy("store_id")
    .agg(
        F.count("revenue").alias("revenue_count"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
    .filter(F.col("revenue_count") >= 5)
    .orderBy("stddev_revenue")
)
