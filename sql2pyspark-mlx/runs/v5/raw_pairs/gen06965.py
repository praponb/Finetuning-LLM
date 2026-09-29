from pyspark.sql import functions as F

matched_keys = (
    df_payments
    .select("method", "order_id")
    .subtract(
        df_payments
        .filter(F.col("paid_at") < "2025-05-01")
        .select("method", "order_id")
    )
)

kept_rows = (
    df_payments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.method") == F.col("k.method")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.when(F.col("method").rlike("^(promptpay|cash)$"), "CORE").otherwise("EXTENDED").alias("method_zone"),
    )
)
