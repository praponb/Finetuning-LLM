from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.month(F.col("a.opened_date")).isin([2, 7, 12]))
    .select(
        F.col("a.balance"),
        F.col("a.account_type"),
        F.col("a.customer_id"),
        F.col("t.merchant"),
        F.col("t.account_id"),
        F.col("t.txn_type"),
    )
)
