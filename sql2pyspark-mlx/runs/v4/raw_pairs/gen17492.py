from pyspark.sql import functions as F

kept_keys = (
    df_payments
    .select("order_id", "id")
    .intersect(
        df_payments
        .filter(F.col("status") == "FAILED")
        .select("order_id", "id")
    )
)

kept_rows = (
    df_payments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("id", "order_id", "amount")
    .orderBy(F.col("id").desc())
)
