from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "left")
    .filter(F.col("transactions.account_id").isin([9, 15, 25, 32]))
    .groupBy(F.col("accounts.id"))
    .agg(F.count(F.col("transactions.id")).alias("transaction_count"))
)
