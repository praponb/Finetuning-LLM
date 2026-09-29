from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("channel").isin(["mobile", "partner"]), "group_a").otherwise("other").alias("channel_group"),
        F.when(F.col("shipping_fee") >= 38, "high").when(F.col("shipping_fee") >= 12, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
    .filter(F.col("amount_range") > 42)
    .select("channel_group", "shipping_fee_bucket", "amount_range")
    .orderBy("channel_group", "shipping_fee_bucket")
    .limit(20)
)
