from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.min("discount").alias("min_discount"),
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
    )
    .filter(F.col("stddev_discount") > 45)
)
