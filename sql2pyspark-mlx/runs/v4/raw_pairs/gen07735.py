from pyspark.sql import functions as F

base = (
    df_payments
    .filter(F.col("order_id").isin([7, 12, 13, 28]))
    .select(
        "id",
        "order_id",
        "method",
        "status",
        F.round("amount", 2).alias("amount_rounded"),
    )
)

base_payments = (
    base
    .filter(F.col("method").isNotNull() & F.col("method").isNotNull())
    .select(
        "*",
        F.length("method").alias("method_length"),
        F.coalesce("method", F.lit("N/A")).alias("method_filled"),
    )
)

enriched = (
    base_payments
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .select("s.*", F.col("o.amount"))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("method_length") >= 62, "PREMIUM").otherwise("STANDARD").alias("method_length_tier"),
        "order_id",
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count("*").alias("event_count"),
        F.round(F.avg("amount_rounded"), 2).alias("avg_amount_rounded"),
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
    )
    .orderBy("avg_amount_rounded")
)
