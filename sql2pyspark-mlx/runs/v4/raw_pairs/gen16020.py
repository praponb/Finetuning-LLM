from pyspark.sql import functions as F

remaining_keys = (
    df_payments
    .select("order_id", "id")
    .intersect(
        df_payments
        .filter(F.col("paid_at") < "2025-08-15")
        .select("order_id", "id")
    )
)

reconciled = (
    df_payments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        "order_id",
        F.when(F.col("amount") >= 1380, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.sum(F.when(F.col("method") == "promptpay", F.col("amount"))).alias("promptpay_total_amount"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min_2"),
    )
)
