from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("transactions.account_id") == F.col("accounts.id"), "inner")
    .filter((F.col("accounts.opened_date") > "2024-10-01") & ~F.col("accounts.status").isin(["CLOSED", "OPEN"]))
    .select(
        F.col("accounts.status"),
        F.col("accounts.opened_date"),
        F.col("accounts.customer_id"),
        F.col("transactions.txn_type"),
        F.col("transactions.is_flagged"),
        F.col("transactions.account_id"),
    )
)
