from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "outer")
    .filter(F.col("transactions.txn_type").isin(["FEE", "CREDIT"]) & (F.col("transactions.txn_type") == "FEE"))
    .select(
        F.col("transactions.id"),
        F.col("transactions.account_id"),
        F.col("accounts.status"),
        F.col("accounts.balance"),
    )
    .orderBy(F.col("id").desc(), F.col("balance").desc())
)
