from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("channel").isin(["mobile", "partner"]), "tier_1").otherwise("other").alias("channel_group"),
        "channel",
    )
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
    .filter(F.col("stddev_amount") >= 52)
    .orderBy(F.col("stddev_amount").desc(), F.col("channel_group").desc())
)
