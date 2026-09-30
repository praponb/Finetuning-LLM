from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .groupBy(F.year(F.col("a.opened_date")).alias("opened_year"))
    .agg(
        F.avg(F.col("t.amount")).alias("avg_amount"),
        F.count(F.col("t.amount")).alias("amount_count"),
    )
    .orderBy(F.col("amount_count").desc())
)
