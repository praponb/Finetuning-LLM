from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .groupBy(F.col("c.segment"))
    .agg(F.count(F.col("a.id")).alias("account_count"))
    .orderBy(F.col("account_count").desc(), "segment")
)
