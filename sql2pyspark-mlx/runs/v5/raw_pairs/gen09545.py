from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("accounts.id") == F.col("transactions.account_id"), "left")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .groupBy(F.col("accounts.status"))
    .agg(F.max(F.col("transactions.txn_time")).alias("last_txn_time"))
    .orderBy("last_txn_time")
)
