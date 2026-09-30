from pyspark.sql import functions as F

kept_keys = (
    df_payments
    .select("id", "status")
    .subtract(
        df_payments
        .filter(F.hour("paid_at").between(9, 17))
        .select("id", "status")
    )
)

verified = (
    df_payments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = verified.select("id", "order_id")
