from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 4, 6]))
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
        F.count("revenue").alias("revenue_count"),
    )
    .orderBy(F.col("revenue_count").desc(), "store_id")
)
