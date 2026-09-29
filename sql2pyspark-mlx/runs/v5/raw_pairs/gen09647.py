from pyspark.sql import functions as F

matched_keys = (
    df_payments
    .select("order_id", "status")
    .subtract(
        df_payments
        .filter(F.col("order_id").isin([8, 38]))
        .select("order_id", "status")
    )
)

kept_rows = (
    df_payments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("id", "order_id", "status")
    .orderBy(F.col("status").desc_nulls_first())
)
