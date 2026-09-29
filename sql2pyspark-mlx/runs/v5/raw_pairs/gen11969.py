from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.col("merchant") == "7-Eleven")
    .groupBy("account_id")
    .agg(
        F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_count"),
        F.count("amount").alias("amount_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .filter(F.col("as.amount_count") >= 4)
    .select(F.col("a.customer_id"), F.col("as.credit_count"), F.col("as.amount_count"))
)
