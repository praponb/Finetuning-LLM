from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.status").isin(["CLOSED", "FROZEN", "OPEN"]))
    .select(F.col("a.balance"), F.col("t.amount"), F.col("t.txn_type"))
)
