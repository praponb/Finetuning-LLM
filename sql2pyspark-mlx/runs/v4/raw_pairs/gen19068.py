from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .filter(F.col("a.balance").between(22000, 51000) & F.col("a.balance").between(18000, 30000))
    .select(
        F.col("t.txn_type"),
        F.col("t.amount"),
        F.col("t.is_flagged"),
        F.col("a.status"),
    )
)
