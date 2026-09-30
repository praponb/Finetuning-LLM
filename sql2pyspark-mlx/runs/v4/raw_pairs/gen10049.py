from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.filter(F.col("txn_time") < "2024-01-01").alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "left_anti")
    .select(
        F.col("accounts.id"),
        F.col("accounts.opened_date"),
        F.col("accounts.status"),
    )
)
