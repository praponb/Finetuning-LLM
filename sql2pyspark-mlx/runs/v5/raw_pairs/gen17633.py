from pyspark.sql import functions as F

attributed = (
    df_orders
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.order_date") >= F.col("sub.start_date")) & (F.col("s.order_date") <= F.date_add(F.col("sub.end_date"), 7)), "inner")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

result = (
    attributed
    .groupBy(
        F.when(F.col("monthly_fee") >= 189, "LARGE").otherwise("SMALL").alias("monthly_fee_tier"),
        "status",
    )
    .agg(
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.round(F.sum("discount") / F.nullif(F.count(F.when(F.col("channel") == "mobile", 1)), F.lit(0)), 2).alias("discount_per_mobile"),
    )
    .filter(F.col("num_channels") >= 1)
    .orderBy(F.col("discount_per_mobile").desc())
)
