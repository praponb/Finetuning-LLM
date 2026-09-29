from pyspark.sql import functions as F

amount_by_account_id = df_transactions.groupBy("account_id").agg(F.avg("amount").alias("avg_transaction_amount"))

with_dims = (
    df_transactions
    .alias("a")
    .join(df_accounts.alias("b"), F.col("a.account_id") == F.col("b.id"), "inner")
    .filter(F.col("b.opened_date").between("2025-07-01", "2025-08-01"))
    .select(
        "a.id",
        "a.account_id",
        "a.txn_time",
        "a.merchant",
        "a.is_flagged",
        F.col("b.opened_date"),
    )
)

result = (
    with_dims
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_time",
        "is_flagged",
        "opened_date",
        F.col("avg_transaction_amount"),
    )
)
