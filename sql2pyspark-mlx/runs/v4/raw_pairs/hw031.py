from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .select(F.col("c.id"), F.col("c.name"), F.col("o.id").alias("order_id"))
)
