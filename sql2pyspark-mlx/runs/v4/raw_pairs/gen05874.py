from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.city"))
    .agg(F.count(F.col("a.balance")).alias("balance_count"))
    .orderBy("balance_count")
)
