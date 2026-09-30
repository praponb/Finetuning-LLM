from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .groupBy(F.year(F.col("a.opened_date")).alias("opened_year"))
    .agg(
        F.round(F.stddev(F.col("t.amount")), 2).alias("stddev_amount"),
        F.min(F.col("t.amount")).alias("min_amount"),
    )
)
