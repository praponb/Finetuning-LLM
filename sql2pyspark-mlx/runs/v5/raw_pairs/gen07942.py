from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel") != "store")
    .groupBy(
        F.when(F.col("channel").isin(["mobile", "store"]), "priority").otherwise("other").alias("channel_group"),
    )
    .agg(F.round(F.stddev("discount"), 2).alias("stddev_discount"))
    .filter(F.col("stddev_discount") < 61)
    .select("channel_group", "stddev_discount")
)
