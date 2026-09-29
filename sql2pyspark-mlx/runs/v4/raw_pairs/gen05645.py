from pyspark.sql import functions as F

amount_by_account_id = df_transactions.groupBy("account_id").agg(F.avg("amount").alias("avg_transaction_amount"))

kept_keys = (
    df_transactions
    .select("account_id", "id")
    .intersect(
        df_transactions
        .filter(F.hour("txn_time").between(11, 14))
        .select("account_id", "id")
    )
)

verified = (
    df_transactions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.account_id") == F.col("k.account_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "is_flagged",
        F.col("avg_transaction_amount"),
    )
    .orderBy(F.col("amount").desc_nulls_first())
)
