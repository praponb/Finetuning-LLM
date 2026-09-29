from pyspark.sql import functions as F

remaining_keys = (
    df_payments
    .select("id", "method")
    .exceptAll(
        df_payments
        .filter(F.col("order_id").isin([30, 34, 40]))
        .select("id", "method")
    )
)

verified = (
    df_payments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.method") == F.col("k.method")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("order_id", "amount", "paid_at")
    .orderBy(F.col("paid_at").desc_nulls_first())
)
