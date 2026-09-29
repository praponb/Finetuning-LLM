from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("txn_type") == "DEBIT", 1).otherwise(0)).alias("debit_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .select(F.col("a.id"), F.col("a.customer_id"), F.col("t.debit_count"))
    .orderBy("id", F.col("debit_count").desc())
)
