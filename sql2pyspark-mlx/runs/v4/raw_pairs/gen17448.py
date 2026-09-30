from pyspark.sql import functions as F

base = (
    df_orders
    .filter(F.col("status") == "COMPLETED")
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        (F.col("amount") * F.col("shipping_fee")).alias("amount_with_shipping"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_clean"),
    )
)

result = (
    base
    .withColumn("year_start", F.date_trunc("year", "order_date"))
    .withColumn("week_start", F.date_trunc("week", "order_date"))
    .groupingSets(
        [["status", "year_start", "week_start"], ["status", "year_start"]],
        "status",
        "year_start",
        "week_start",
    )
    .agg(
        F.sum(F.when(F.col("status") == "SHIPPED", F.col("amount_with_shipping")).otherwise(0)).alias("shipped_amount_with_shipping"),
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("week_start").desc())
)
