from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(F.min("amount").alias("min_amount"))
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "left")
    .filter(F.col("tba.min_amount") >= 2100)
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("a.balance"),
        F.col("tba.min_amount"),
    )
    .orderBy("min_amount")
)
