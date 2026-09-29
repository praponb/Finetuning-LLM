from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("t.merchant").isin(["Grab", "Amazon", "Starbucks"]) | F.col("a.status").isin(["FROZEN", "CLOSED", "OPEN"]))
    .select(
        F.col("a.status"),
        F.col("a.balance"),
        F.col("t.is_flagged"),
        F.col("t.txn_time"),
        F.col("t.merchant"),
        F.substring(F.col("a.status"), 1, 2).alias("status_prefix"),
    )
    .orderBy("txn_time")
)
