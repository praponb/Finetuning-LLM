from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.age").between(36, 64) & (F.col("o.shipping_fee") < 37))
    .select(F.col("s.order_id"), F.col("o.shipping_fee"), F.col("c.signup_date"))
    .orderBy("order_id")
)
