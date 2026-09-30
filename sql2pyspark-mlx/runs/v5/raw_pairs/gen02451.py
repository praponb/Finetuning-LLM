from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy("account_id")
    .agg(
        F.min("amount").alias("min_amount"),
        F.count("*").alias("total_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .filter(F.col("as.total_count") >= 6)
    .select(F.col("a.account_type"), F.col("as.min_amount"), F.col("as.total_count"))
    .orderBy("account_type")
)
