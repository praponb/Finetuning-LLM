from pyspark.sql import functions as F

kept_keys = (
    df_transactions
    .select("txn_type", "account_id")
    .exceptAll(
        df_transactions
        .filter(F.col("account_id") == 22)
        .select("txn_type", "account_id")
    )
)

verified = (
    df_transactions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")) & (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("amount", "txn_time", "is_flagged")
    .orderBy(F.col("txn_time").desc_nulls_last(), "amount")
)
