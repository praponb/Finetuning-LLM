from pyspark.sql import functions as F

matched_keys = (
    df_payments
    .select("id")
    .intersect(
        df_payments
        .filter(F.col("amount") > 670)
        .select("id")
    )
)

verified = (
    df_payments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = verified.select("method", "amount", "paid_at", "status")
