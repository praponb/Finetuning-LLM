from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.opened_date") <= "2025-09-01")
    .select(
        F.col("t.txn_time"),
        F.col("t.amount"),
        F.col("a.status"),
        F.col("a.customer_id"),
    )
    .orderBy("customer_id")
)
