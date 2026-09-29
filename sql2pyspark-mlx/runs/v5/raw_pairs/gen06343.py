from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("transactions.account_id") == F.col("accounts.id"), "inner")
    .filter(F.col("accounts.opened_date") < "2024-07-01")
    .groupBy(F.col("accounts.id"))
    .agg(F.count(F.col("transactions.id")).alias("transaction_count"))
)
