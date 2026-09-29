from pyspark.sql import functions as F

recent_orders = df_orders.filter(F.col("shipping_fee") <= 26)

orders_stats = (
    recent_orders
    .groupBy(
        "status",
        F.when(F.col("discount") >= 0.33, "high").when(F.col("discount") >= 0.1, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(
        F.avg("shipping_fee").alias("avg_shipping_fee"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
)

result = (
    orders_stats
    .filter(F.col("avg_shipping_fee") < 31)
    .select("status", "discount_bucket", "avg_shipping_fee", "channel_list")
    .orderBy(F.col("avg_shipping_fee").desc())
)
