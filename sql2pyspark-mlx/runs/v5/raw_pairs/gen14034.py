from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(F.countDistinct("amount").alias("unique_merchants"))
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("T"), F.col("a.id") == F.col("T.account_id"), "inner")
    .filter(F.col("T.unique_merchants") >= 5)
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("a.account_type"),
        F.col("T.unique_merchants"),
    )
)
