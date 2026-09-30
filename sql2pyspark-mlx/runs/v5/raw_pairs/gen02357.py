from pyspark.sql import functions as F

valid_accounts = df_accounts.filter(F.col("customer_id").isin([2, 24, 32, 39]))

accounts_stats = (
    valid_accounts
    .groupBy("customer_id")
    .agg((F.max("balance") - F.min("balance")).alias("balance_range"))
)

result = (
    accounts_stats
    .orderBy(F.col("customer_id").desc())
    .limit(1)
)
