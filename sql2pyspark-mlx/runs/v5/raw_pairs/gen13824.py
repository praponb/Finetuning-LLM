from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "left")
    .filter((F.col("accounts.status") == "OPEN") & (F.year(F.col("accounts.opened_date")) >= 2023))
    .select(
        F.col("transactions.is_flagged"),
        F.col("transactions.amount"),
        F.col("transactions.merchant"),
        F.col("accounts.balance"),
        F.col("accounts.opened_date"),
    )
)
