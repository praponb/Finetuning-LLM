from pyspark.sql import functions as F

filtered_orders = (
    df_orders
    .filter((F.col("channel") == "mobile") & F.col("store_id").isin([4, 9, 29, 40]))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("N/A")))
)

channel_summary = (
    filtered_orders
    .groupBy("channel", "customer_id")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.sum("discount").alias("total_discount"),
    )
)

result = (
    channel_summary
    .filter(F.col("total_discount") >= 2.3)
    .orderBy(F.col("customer_id").desc(), F.col("total_discount").desc())
)
