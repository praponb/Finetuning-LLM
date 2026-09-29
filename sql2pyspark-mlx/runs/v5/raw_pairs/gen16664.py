from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.col("amount").between(1300, 2500))
    .groupBy("account_id")
    .agg(F.sum("amount").alias("total_amount"))
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "inner")
    .select(F.col("a.opened_date"), F.col("a.status"), F.col("at.total_amount"))
)
