from pyspark.sql import functions as F

cleaned = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-11-01").cast("date")) & F.col("method").isNotNull())
    .select("id", "order_id", "status", F.trim("method").alias("clean_method"))
)

remaining_keys = (
    cleaned
    .select("status", "id")
    .exceptAll(
        df_payments
        .filter(F.col("status") != "FAILED")
        .select("status", "id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .select(
        "e.*",
        F.col("d.channel"),
        F.col("d.discount"),
        F.col("d.status").alias("order_status"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("discount") >= 0.45, "HIGH").otherwise("LOW").alias("discount_tier"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "REFUNDED", F.col("discount"))).alias("refunded_total_discount"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.max(F.when(F.col("order_status") == "PENDING", F.col("discount"))).alias("pending_max_discount"),
    )
)
