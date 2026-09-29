from pyspark.sql import functions as F

matched_keys = (
    df_payments
    .select("status")
    .intersect(
        df_payments
        .filter(F.col("status").isin(["REFUNDED", "SUCCESS", "FAILED"]))
        .select("status")
    )
)

kept_rows = (
    df_payments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("id", "order_id", "amount")
    .orderBy(F.col("amount").desc_nulls_first(), "order_id")
)
