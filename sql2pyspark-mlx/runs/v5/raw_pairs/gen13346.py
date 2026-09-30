from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("amount")).otherwise(0)).alias("debit_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .filter(F.col("as.debit_amount") >= 51)
    .select(F.col("a.status"), F.col("as.total_amount"), F.col("as.debit_amount"))
    .orderBy(F.col("debit_amount").desc(), F.col("total_amount").desc())
)
