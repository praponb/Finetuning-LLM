from pyspark.sql import functions as F

valid_orders = (
    df_orders
    .filter(F.col("store_id").isin([6, 20, 29, 36]) & (F.col("order_date") >= "2023-06-01") & F.col("store_id").isin([3, 12, 15, 38]))
    .withColumn("order_date_dayofweek", F.dayofweek("order_date"))
)

channel_summary = (
    valid_orders
    .groupBy(
        "channel",
        F.when(F.col("discount") >= 0.42, "high").when(F.col("discount") >= 0.2, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(F.round(F.avg("discount"), 2).alias("avg_discount"))
)

result = (
    channel_summary
    .select("channel", "discount_bucket", "avg_discount")
    .orderBy("avg_discount")
)
