from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("a.customer_id").isNotNull())
    .groupBy(F.col("c.city"), F.col("c.country"))
    .agg(F.round(F.avg(F.col("a.balance")), 2).alias("avg_balance"))
    .filter(F.col("avg_balance") >= 79)
    .orderBy("city")
)
