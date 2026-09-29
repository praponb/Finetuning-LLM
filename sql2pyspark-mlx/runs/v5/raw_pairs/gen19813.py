from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(F.sum("amount").alias("total_amount"))
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("t.total_amount") >= 19000)
    .select(F.col("a.opened_date"), F.col("a.balance"), F.col("t.total_amount"))
    .orderBy(F.col("total_amount").desc())
)
