from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("transactions.account_id") == F.col("accounts.id"), "left")
    .filter(~F.col("accounts.status").isin(["OPEN", "CLOSED"]) & (F.col("transactions.txn_type") == "CREDIT"))
    .select(
        F.col("accounts.account_type"),
        F.col("accounts.customer_id"),
        F.col("accounts.status"),
        F.col("transactions.merchant"),
    )
)
