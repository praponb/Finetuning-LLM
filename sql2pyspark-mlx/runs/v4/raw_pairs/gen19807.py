from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "outer")
    .filter((F.col("a.account_type") == "LOAN") | F.hour(F.col("t.txn_time")).between(10, 17))
    .select(
        F.col("t.id"),
        F.col("t.is_flagged"),
        F.col("t.account_id"),
        F.col("t.amount"),
        F.col("a.customer_id"),
        F.col("a.balance"),
        F.col("a.account_type"),
    )
)
