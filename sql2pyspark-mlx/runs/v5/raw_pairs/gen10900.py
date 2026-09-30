from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([5, 8, 9, 10]))
    .agg(
        F.avg("units").alias("avg_units"),
        F.sum("revenue").alias("total_revenue"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
    )
)
