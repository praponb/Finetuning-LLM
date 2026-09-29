from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .groupBy("account_id")
    .agg(F.min("amount").alias("min_amount"))
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("A"), F.col("a.id") == F.col("A.account_id"), "left")
    .filter(F.col("A.min_amount") >= 1900)
    .select(F.col("a.customer_id"), F.col("a.balance"), F.col("A.min_amount"))
)
