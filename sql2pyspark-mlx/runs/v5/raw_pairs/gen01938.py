from pyspark.sql import functions as F

amount_by_account_id = df_transactions.groupBy("account_id").agg(F.max("amount").alias("max_transaction_amount"))

remaining_keys = (
    df_transactions
    .select("account_id", "id")
    .intersect(
        df_transactions
        .filter(F.hour("txn_time").between(12, 17))
        .select("account_id", "id")
    )
)

verified = (
    df_transactions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.account_id") == F.col("k.account_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.col("max_transaction_amount"),
    )
)
