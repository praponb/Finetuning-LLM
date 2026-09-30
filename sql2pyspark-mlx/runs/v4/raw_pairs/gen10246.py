from pyspark.sql import functions as F

orders_by_customer = (
    df_orders
    .filter(F.col("status").isin(["RETURNED", "SHIPPED", "CANCELLED"]))
    .groupBy("customer_id")
    .agg(F.count("*").alias("num_orders"))
)

result = (
    df_customers.alias("c")
    .join(orders_by_customer.alias("obc"), F.col("c.id") == F.col("obc.customer_id"), "inner")
    .select(F.col("c.name"), F.col("obc.num_orders"))
    .orderBy(F.col("num_orders").desc())
)
