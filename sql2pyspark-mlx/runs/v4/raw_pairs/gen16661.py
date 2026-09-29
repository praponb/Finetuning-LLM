from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .filter(F.col("o.status").isin(["SHIPPED", "RETURNED", "CANCELLED"]))
    .select(F.col("o.id"), F.col("o.amount"), F.col("c.city"), F.col("c.country"))
)
