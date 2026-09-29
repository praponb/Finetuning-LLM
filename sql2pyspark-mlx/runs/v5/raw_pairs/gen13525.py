from pyspark.sql import functions as F

orders_by_customer = (
    df_orders
    .groupBy("customer_id")
    .agg((F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"))
)

result = (
    df_customers.alias("c")
    .join(orders_by_customer.alias("obc"), F.col("c.id") == F.col("obc.customer_id"), "left")
    .filter(F.col("obc.shipping_fee_range") >= 35)
    .select(F.col("c.name"), F.col("c.email"), F.col("obc.shipping_fee_range"))
    .orderBy("email")
)
