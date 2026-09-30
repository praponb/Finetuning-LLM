from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("t.txn_type").isin(["FEE", "DEBIT", "CREDIT"]) & (F.col("t.txn_type") == "CREDIT"))
    .select(
        F.col("a.status"),
        F.col("t.is_flagged"),
        F.col("t.account_id"),
        F.col("t.merchant"),
    )
    .orderBy("status")
)
