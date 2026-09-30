from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "left")
    .select(
        F.col("transactions.amount"),
        F.col("transactions.txn_time"),
        F.col("transactions.merchant"),
        F.col("accounts.customer_id"),
        F.col("accounts.balance"),
    )
)
