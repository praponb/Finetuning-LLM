from pyspark.sql import functions as F

valid = (
    df_payments
    .filter(F.col("paid_at") >= F.lit("2023-06-22").cast("date"))
    .withColumn("is_refunded", F.when(F.col("status") == "REFUNDED", 1).otherwise(0))
)

remaining_keys = (
    valid
    .select("method", "status")
    .exceptAll(
        df_payments
        .filter(F.col("method") == "cash")
        .select("method", "status")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.method") == F.col("k.method")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .select("a.*", F.col("o.order_date"))
)

matched_keys = (
    with_dims
    .select("order_id", "status")
    .exceptAll(
        df_payments
        .filter(F.col("order_id").isNotNull())
        .select("order_id", "status")
    )
)

verified = (
    with_dims
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("status")
    .agg(
        F.countDistinct("method").alias("distinct_methods"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.size(F.collect_set("method")).alias("num_methods"),
        F.count_if(F.col("method") == "promptpay").alias("promptpay_events"),
    )
    .orderBy(F.col("status").desc_nulls_last())
)
