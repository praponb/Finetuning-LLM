from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "right")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .select(
        F.col("transactions.id"),
        F.col("transactions.merchant"),
        F.col("transactions.txn_time"),
        F.col("accounts.status"),
        F.col("customers.city"),
        F.col("customers.is_active"),
    )
    .orderBy("txn_time", "city")
)
