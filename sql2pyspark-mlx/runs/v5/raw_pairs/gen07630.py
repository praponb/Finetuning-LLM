from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.col("amount") >= 2200)
    .groupBy("account_id")
    .agg(F.min("txn_time").alias("first_txn_time"))
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "left")
    .select(F.col("a.id"), F.col("a.status"), F.col("at.first_txn_time"))
    .orderBy("status")
)
