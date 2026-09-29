from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "left")
    .select(
        F.col("transactions.txn_type"),
        F.col("transactions.amount"),
        F.col("transactions.merchant"),
        F.col("accounts.customer_id"),
        F.round(F.col("transactions.amount"), 0).alias("amount_rounded"),
    )
)
