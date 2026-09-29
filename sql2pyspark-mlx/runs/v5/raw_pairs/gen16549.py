from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("account_id").isin([2, 34]))
    .groupBy("account_id")
    .agg(
        F.countDistinct("amount").alias("unique_accounts"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "inner")
    .filter(F.col("tba.avg_amount") >= 37)
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("tba.unique_accounts"),
        F.col("tba.avg_amount"),
    )
)
