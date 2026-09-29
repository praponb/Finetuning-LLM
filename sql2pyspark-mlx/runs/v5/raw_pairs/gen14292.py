from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(F.min("amount").alias("min_amount"))
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("T"), F.col("a.id") == F.col("T.account_id"), "left")
    .filter(F.col("T.min_amount") >= 1800)
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("a.status"),
        F.col("T.min_amount"),
    )
    .orderBy(F.col("id").desc())
)
