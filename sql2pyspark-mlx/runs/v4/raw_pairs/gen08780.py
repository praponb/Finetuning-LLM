from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.hour(F.col("t.txn_time")).between(9, 16) & F.col("t.txn_type").isin(["DEBIT", "CREDIT", "FEE"]))
    .select(
        F.col("t.id"),
        F.col("t.is_flagged"),
        F.col("t.account_id"),
        F.col("a.customer_id"),
        F.col("a.status"),
        F.col("a.balance"),
    )
)
