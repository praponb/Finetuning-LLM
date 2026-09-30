from pyspark.sql import functions as F

matched_keys = (
    df_transactions
    .select("merchant", "id")
    .intersect(
        df_transactions
        .filter(~F.col("is_flagged"))
        .select("merchant", "id")
    )
)

verified = (
    df_transactions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.merchant") == F.col("k.merchant")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("id", "account_id", "txn_type", "amount", "txn_time", "merchant")
    .orderBy("merchant")
)
