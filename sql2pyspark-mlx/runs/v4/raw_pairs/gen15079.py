from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(~F.col("txn_type").isin(["REFUND", "FEE", "DEBIT"]))
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("amount")).otherwise(0)).alias("debit_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .filter(F.col("as.debit_amount") > 41)
    .select(F.col("a.status"), F.col("a.customer_id"), F.col("as.debit_amount"))
    .orderBy("status")
)
