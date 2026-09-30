from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.to_date("txn_time") >= "2024-11-15")
    .groupBy("account_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("tba.total_amount"),
        F.col("tba.avg_amount"),
    )
    .orderBy(F.col("id").desc())
)
