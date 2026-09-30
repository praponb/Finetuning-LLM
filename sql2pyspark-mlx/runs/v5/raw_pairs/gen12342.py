from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("txn_type").isin(["CREDIT", "DEBIT", "REFUND"]))
    .groupBy("account_id")
    .agg(F.count("*").alias("num_transactions"))
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("A"), F.col("a.id") == F.col("A.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("a.customer_id"),
        F.col("A.num_transactions"),
    )
)
