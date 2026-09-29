from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        "channel",
        "status",
        F.date_format("order_date", "yyyy-MM").alias("order_month"),
    )
    .agg(
        F.avg("shipping_fee").alias("avg_shipping_fee"),
        F.count("discount").alias("discount_count"),
        F.round(((100 * F.sum(F.when(F.col("channel") == "mobile", 1).otherwise(0))) / F.count("*")), 1).alias("pct_mobile"),
    )
)
