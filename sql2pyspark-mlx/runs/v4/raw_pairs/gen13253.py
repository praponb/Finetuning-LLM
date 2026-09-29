from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.col("amount") > 2300)
    .groupBy("account_id")
    .agg(F.max("amount").alias("max_amount"))
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .filter(F.col("as.max_amount") > 1200)
    .select(F.col("a.id"), F.col("a.opened_date"), F.col("as.max_amount"))
    .orderBy(F.col("id").desc(), "max_amount")
)
