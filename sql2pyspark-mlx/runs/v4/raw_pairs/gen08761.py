from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("store_id", "channel")
    .agg(
        F.sum(F.when(F.col("channel") == "web", 1).otherwise(0)).alias("web_count"),
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
    )
    .filter(F.col("web_count") >= 76)
)
