from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("account_id").isin([8, 10, 17, 36]))
    .groupBy("account_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.max("amount").alias("max_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("A"), F.col("a.id") == F.col("A.account_id"), "left")
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("A.total_amount"),
        F.col("A.max_amount"),
    )
    .orderBy(F.col("max_amount").desc())
)
