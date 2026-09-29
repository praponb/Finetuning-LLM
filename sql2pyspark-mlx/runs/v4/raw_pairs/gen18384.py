from pyspark.sql import functions as F

orders_by_customer = (
    df_orders
    .filter(F.col("order_date") >= "2025-06-01")
    .groupBy("customer_id")
    .agg(F.sum("shipping_fee").alias("total_shipping_fee"))
)

result = (
    df_customers.alias("c")
    .join(orders_by_customer.alias("obc"), F.col("c.id") == F.col("obc.customer_id"), "inner")
    .filter(F.col("obc.total_shipping_fee") >= 240)
    .select(F.col("c.is_active"), F.col("obc.total_shipping_fee"))
    .orderBy(F.col("total_shipping_fee").desc())
)
