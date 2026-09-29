from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .groupBy("account_id")
    .agg(F.min("amount").alias("min_amount"))
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .select(F.col("a.id"), F.col("a.opened_date"), F.col("t.min_amount"))
    .orderBy("id")
)
