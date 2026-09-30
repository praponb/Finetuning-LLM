from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "outer")
    .groupBy(F.col("c.is_active"))
    .agg((F.max(F.col("a.balance")) - F.min(F.col("a.balance"))).alias("balance_range"))
)
