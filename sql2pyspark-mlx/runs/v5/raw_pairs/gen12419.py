from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.segment"))
    .agg(F.avg(F.col("a.balance")).alias("avg_balance"))
    .filter(F.col("avg_balance") > 40000)
)
