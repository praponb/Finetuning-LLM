from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(~F.col("merchant").isin(["Lazada", "Starbucks", "Grab"]))
    .groupBy("account_id")
    .agg(
        F.min("txn_time").alias("min_txn_time"),
        F.sum(F.when(F.col("txn_type") == "FEE", 1).otherwise(0)).alias("fee_count"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(account_summary.alias("account_summary"), F.col("accounts.id") == F.col("account_summary.account_id"), "left")
    .filter(F.col("account_summary.fee_count") > 69)
    .select(
        F.col("accounts.id"),
        F.col("accounts.status"),
        F.col("account_summary.min_txn_time"),
        F.col("account_summary.fee_count"),
    )
    .orderBy(F.col("id").desc(), "fee_count")
)
