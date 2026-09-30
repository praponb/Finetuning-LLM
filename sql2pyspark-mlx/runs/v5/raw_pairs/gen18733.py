from pyspark.sql import functions as F

cleaned = (
    df_orders
    .filter(F.col("status") == "RETURNED")
    .select(
        "*",
        F.last_day("order_date").alias("order_date_month_end"),
        F.round((F.col("discount") / F.col("amount")), 2).alias("discount_per_amount"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("amount") >= 1370, "HIGH").otherwise("LOW").alias("amount_tier"),
        F.when(F.col("discount") >= 0.36, "PREMIUM").otherwise("STANDARD").alias("discount_tier"),
        F.when(F.col("shipping_fee") >= 34, "PREMIUM").otherwise("STANDARD").alias("shipping_fee_tier"),
    )
    .agg(
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_count"),
        F.corr("shipping_fee", "discount").alias("corr_shipping_fee_discount"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.round(F.avg("discount"), 2).alias("avg_discount"),
    )
    .orderBy(F.col("avg_discount").desc_nulls_last())
)
