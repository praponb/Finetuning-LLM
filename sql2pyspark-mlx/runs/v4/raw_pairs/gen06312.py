from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").between(0.24, 0.39))
    .groupBy(
        F.when(F.col("channel").isin(["store", "partner"]), "group_a").otherwise("other").alias("channel_group"),
        "customer_id",
    )
    .agg(F.round(F.stddev("discount"), 2).alias("stddev_discount"))
    .orderBy(
        F.col("channel_group").desc(),
        F.col("stddev_discount").desc(),
        "customer_id",
    )
    .limit(5)
)
