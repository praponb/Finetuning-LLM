from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("account_id") == 35)
    .groupBy("account_id")
    .agg(F.countDistinct("amount").alias("unique_merchants"))
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.unique_merchants") > 8)
    .select(F.col("a.status"), F.col("t.unique_merchants"))
    .orderBy("unique_merchants")
)
