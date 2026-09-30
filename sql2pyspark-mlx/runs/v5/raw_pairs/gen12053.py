from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("cnt"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("A"), F.col("a.id") == F.col("A.account_id"), "inner")
    .filter(F.col("A.cnt") >= 3)
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("a.balance"),
        F.col("A.total_amount"),
        F.col("A.cnt"),
    )
)
