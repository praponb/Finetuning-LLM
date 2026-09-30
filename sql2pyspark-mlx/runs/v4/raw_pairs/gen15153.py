from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("accounts.id") == F.col("transactions.account_id"), "inner")
    .filter(F.col("accounts.customer_id").isNull())
    .select(
        F.col("transactions.account_id"),
        F.col("transactions.amount"),
        F.col("accounts.status"),
    )
)
