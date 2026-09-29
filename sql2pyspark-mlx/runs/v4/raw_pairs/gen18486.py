from pyspark.sql import functions as F

amount_by_account_id = df_transactions.groupBy("account_id").agg(F.sum("amount").alias("sum_transaction_amount"))

matched_keys = (
    df_transactions
    .select("account_id", "id")
    .exceptAll(
        df_transactions
        .filter(F.col("account_id").isin([18, 20, 27]))
        .select("account_id", "id")
    )
)

reconciled = (
    df_transactions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.account_id") == F.col("k.account_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        F.col("sum_transaction_amount"),
    )
    .orderBy(F.col("sum_transaction_amount").desc(), "amount")
)
