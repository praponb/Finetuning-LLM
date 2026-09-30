from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.to_date("txn_time") < "2024-06-01")
    .groupBy("account_id")
    .agg(F.sum("amount").alias("total_amount"))
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "inner")
    .filter(F.col("at.total_amount") >= 24000)
    .select(F.col("a.status"), F.col("a.customer_id"), F.col("at.total_amount"))
)
