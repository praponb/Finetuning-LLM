from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.month(F.col("a.opened_date")).alias("opened_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("t.merchant")))).alias("merchant_values"),
        F.round(F.avg(F.col("t.amount")), 2).alias("avg_amount"),
    )
    .filter(F.col("avg_amount") >= 40)
)
