from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("t.is_flagged"))
    .groupBy(F.year(F.col("a.opened_date")).alias("opened_year"))
    .agg(
        F.round(F.stddev(F.col("t.amount")), 2).alias("stddev_amount"),
        F.round(F.avg(F.col("t.amount")), 2).alias("avg_amount"),
    )
    .orderBy(F.col("opened_year").desc(), F.col("stddev_amount").desc())
)
