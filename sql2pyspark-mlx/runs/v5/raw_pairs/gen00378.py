from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "left")
    .filter(F.col("transactions.merchant").isNotNull() | F.col("transactions.account_id").isin([23, 32, 35, 36]))
    .select(
        F.col("accounts.status"),
        F.col("transactions.account_id"),
        F.to_date(F.col("transactions.txn_time")).alias("txn_time_date"),
    )
    .orderBy(F.col("status").desc())
)
