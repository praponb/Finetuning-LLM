from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("accounts.id") == F.col("transactions.account_id"), "left")
    .filter(F.col("accounts.opened_date") < "2025-11-01")
    .select(
        F.col("transactions.id"),
        F.col("transactions.amount"),
        F.col("transactions.is_flagged"),
        F.col("transactions.account_id"),
        F.col("accounts.opened_date"),
        F.col("accounts.status"),
    )
)
