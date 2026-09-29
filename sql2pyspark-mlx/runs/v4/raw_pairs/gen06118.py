from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "left")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "right")
    .select(
        F.col("transactions.amount"),
        F.col("accounts.balance"),
        F.col("accounts.customer_id"),
        F.col("customers.name"),
        F.hour(F.col("transactions.txn_time")).alias("txn_time_hour"),
    )
)
