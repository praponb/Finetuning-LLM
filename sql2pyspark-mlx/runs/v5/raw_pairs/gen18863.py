from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .groupBy("account_id")
    .agg(F.count("*").alias("num_transactions"))
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .select(F.col("a.customer_id"), F.col("as.num_transactions"))
)
