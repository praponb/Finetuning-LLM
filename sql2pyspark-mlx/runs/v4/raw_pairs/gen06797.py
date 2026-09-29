from pyspark.sql import functions as F

result = (
    df_orders
    .agg(
        F.sum(F.when(F.col("status") == "SHIPPED", 1).otherwise(0)).alias("shipped_count"),
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
    )
    .orderBy(F.col("stddev_discount").desc())
)
