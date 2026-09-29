from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .filter(F.col("o.amount") <= 1130)
    .select(F.col("o.id"), F.col("o.store_id"), F.col("c.is_active"))
)
