from pyspark.sql import functions as F

amount_by_account_id = df_transactions.groupBy("account_id").agg(F.avg("amount").alias("avg_transaction_amount"))

joined = (
    df_transactions
    .alias("a")
    .join(df_accounts.alias("p"), F.col("a.account_id") == F.col("p.id"), "left")
    .filter(F.col("p.balance").between(23000, 67000))
    .select("a.*", F.col("p.balance"))
)

result = (
    joined
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "balance",
        F.col("avg_transaction_amount"),
    )
    .orderBy("id", "balance")
)
