from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "outer")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .filter((F.col("o.shipping_fee") <= 18) & F.upper(F.col("c.email")).like("%LIC%"))
    .select(
        F.col("oi.quantity"),
        F.col("oi.order_id"),
        F.col("o.status"),
        F.col("c.age"),
        F.col("c.credit_limit"),
    )
)
