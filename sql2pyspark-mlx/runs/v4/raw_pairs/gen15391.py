from pyspark.sql import functions as F

recent_accounts = df_accounts.filter(F.col("account_type") != "CREDIT")

accounts_stats = (
    recent_accounts
    .groupBy("status", "account_type")
    .agg(F.round(F.avg("balance"), 2).alias("avg_balance"))
)

result = (
    accounts_stats
    .filter(F.col("avg_balance") > 61)
    .orderBy("account_type")
)
