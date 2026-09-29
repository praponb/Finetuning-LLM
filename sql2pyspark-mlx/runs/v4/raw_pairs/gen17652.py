from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.age").isNotNull() & (F.col("oi.order_id") == 8))
    .select(
        F.col("oi.quantity"),
        F.col("oi.unit_price"),
        F.col("o.status"),
        F.col("o.channel"),
        F.col("c.name"),
        F.col("c.country"),
    )
)
