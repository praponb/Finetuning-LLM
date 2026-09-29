from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.col("txn_type") == "CREDIT")
    .groupBy("account_id")
    .agg(F.sum("amount").alias("total_amount"))
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "left")
    .filter(F.col("at.total_amount") > 12000)
    .select(F.col("a.id"), F.col("a.status"), F.col("at.total_amount"))
)
