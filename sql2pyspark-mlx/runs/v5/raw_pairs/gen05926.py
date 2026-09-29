from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy("account_id")
    .agg(
        F.count("amount").alias("amount_count"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .filter(F.col("as.amount_count") > 4)
    .select(F.col("a.opened_date"), F.col("as.amount_count"), F.col("as.amount_range"))
)
