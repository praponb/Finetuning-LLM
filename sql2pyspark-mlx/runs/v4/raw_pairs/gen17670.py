from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.txn_type").isin(["CREDIT", "FEE"]))
    .select(
        F.col("a.status"),
        F.col("a.opened_date"),
        F.col("a.customer_id"),
        F.col("t.txn_time"),
        F.col("t.account_id"),
    )
)
