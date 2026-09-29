from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("merchant") != "Grab")
    .groupBy("account_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.sum(F.when(F.col("txn_type") == "FEE", 1).otherwise(0)).alias("fee_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("t.fee_count") > 53)
    .select(
        F.col("a.balance"),
        F.col("a.status"),
        F.col("t.total_amount"),
        F.col("t.fee_count"),
    )
    .orderBy(F.col("balance").desc())
)
