from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("txn_type") == "FEE")
    .groupBy("account_id")
    .agg(
        F.count("amount").alias("amount_count"),
        F.min("amount").alias("min_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "inner")
    .filter(F.col("tba.min_amount") >= 3800)
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("a.account_type"),
        F.col("tba.amount_count"),
        F.col("tba.min_amount"),
    )
)
