from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("a.customer_id").isin([2, 7, 30, 32]))
    .select(
        F.col("a.opened_date"),
        F.col("a.status"),
        F.col("a.balance"),
        F.col("t.is_flagged"),
        F.col("t.amount"),
        F.col("t.txn_time"),
    )
)
