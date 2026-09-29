from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("merchant").isin(["Grab", "Lazada", "Shell"]))
    .groupBy("account_id")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "inner")
    .filter(F.col("tba.credit_count") >= 3)
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("tba.avg_amount"),
        F.col("tba.credit_count"),
    )
    .orderBy("opened_date", F.col("credit_count").desc())
)
