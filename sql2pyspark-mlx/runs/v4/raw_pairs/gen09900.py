from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("txn_time") < "2024-07-03")
    .groupBy("account_id")
    .agg(F.min("amount").alias("min_amount"))
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("a.account_type"),
        F.col("tba.min_amount"),
    )
    .orderBy(F.col("customer_id").desc(), "id")
    .limit(10)
)
