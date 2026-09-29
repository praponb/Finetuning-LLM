from pyspark.sql import functions as F

matched_keys = (
    df_payments
    .select("id", "order_id")
    .exceptAll(
        df_payments
        .filter(F.col("order_id").isNotNull())
        .select("id", "order_id")
    )
)

verified = (
    df_payments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("id", "method")
    .orderBy(F.col("id").desc_nulls_last(), "method")
)
