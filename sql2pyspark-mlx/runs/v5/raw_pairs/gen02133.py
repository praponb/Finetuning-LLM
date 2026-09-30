from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(16, 23))
    .groupBy("store_id")
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.count("*").alias("num_store_sales"),
    )
    .filter(F.col("avg_revenue") < 53)
)
