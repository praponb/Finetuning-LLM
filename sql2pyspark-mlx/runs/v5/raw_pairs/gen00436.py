from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("t.is_flagged"))
    .groupBy(
        F.col("a.account_type"),
        F.date_trunc("MONTH", F.col("a.opened_date")).alias("opened_month"),
    )
    .agg(F.round(F.stddev(F.col("t.amount")), 2).alias("stddev_amount"))
)
