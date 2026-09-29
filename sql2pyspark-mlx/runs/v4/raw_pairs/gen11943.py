from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(F.col("c.age") < 31)
    .groupBy(F.col("c.id"), F.col("c.name"))
    .agg(F.count(F.col("a.id")).alias("account_count"))
)
