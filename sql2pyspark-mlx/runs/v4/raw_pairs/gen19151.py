from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .groupBy(F.col("a.status"), F.col("a.customer_id"))
    .agg(F.round(F.avg(F.col("t.amount")), 2).alias("avg_amount"))
    .orderBy("customer_id")
)
