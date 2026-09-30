from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.balance") <= 26000)
    .select(
        F.col("t.id"),
        F.col("t.txn_type"),
        F.col("t.account_id"),
        F.col("a.opened_date"),
        F.col("a.status"),
    )
    .orderBy("status")
)
