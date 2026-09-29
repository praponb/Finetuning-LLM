from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy("account_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.max("amount").alias("max_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "inner")
    .filter(F.col("tba.total_amount") >= 19000)
    .select(F.col("a.customer_id"), F.col("tba.total_amount"), F.col("tba.max_amount"))
    .orderBy(F.col("total_amount").desc(), "customer_id")
)
