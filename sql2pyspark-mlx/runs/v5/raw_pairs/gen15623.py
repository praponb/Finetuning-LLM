from pyspark.sql import functions as F

filtered = (
    df_payments
    .filter(F.col("method").isNotNull() & F.col("method").isin(["card", "paypal", "promptpay"]))
    .withColumn("amount_filled", F.coalesce("amount", F.lit(0)))
    .withColumn("amount_filled_2", F.coalesce("amount", F.lit(0)))
    .withColumn("method_clean", F.coalesce("method", F.lit("N/A")))
)

joined = (
    filtered
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.method",
        "s.status",
        "s.amount_filled_2",
        F.col("p.amount").alias("order_amount"),
        F.col("p.discount"),
    )
)

base = (
    joined
    .filter((~F.col("method").eqNullSafe(F.lit("cash"))) & (F.col("status") != "REFUNDED"))
    .select(
        "id",
        "order_id",
        "method",
        "status",
        "order_amount",
        "discount",
        F.when(F.col("method") == "cash", 1).otherwise(0).alias("is_cash"),
        F.coalesce("method", F.lit("OTHER")).alias("method_filled"),
    )
)

remaining_keys = (
    base
    .select("order_id")
    .intersect(
        df_payments
        .filter(F.to_date("paid_at") < "2024-01-01")
        .select("order_id")
    )
)

reconciled = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter(F.col("method").isNotNull() & (F.col("discount") < 0.2))
    .withColumn("order_amount_whole", F.ceil("order_amount"))
    .withColumn("discount_minus_is_cash", (F.col("discount") - F.col("is_cash")))
    .withColumn("resolved_method", F.coalesce("method", F.lit("DIRECT")))
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("is_cash") >= 73, "HIGH").otherwise("LOW").alias("is_cash_tier"),
    )
    .agg(
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"),
        F.sum(F.when(F.col("method") == "card", F.col("order_amount_whole"))).alias("card_total_order_amount_whole"),
        F.count("*").alias("row_count"),
    )
)
