from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.countDistinct("amount").alias("unique_accounts"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .select(
        F.col("a.customer_id"),
        F.col("a.balance"),
        F.col("as.unique_accounts"),
        F.col("as.stddev_amount"),
    )
    .orderBy(F.col("stddev_amount").desc())
)
