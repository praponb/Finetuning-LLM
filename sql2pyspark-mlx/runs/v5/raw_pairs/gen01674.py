from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .filter(F.col("oi.order_id").isin([15, 23, 24, 32]) & (F.col("c.age") < 36))
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("oi.quantity"),
        F.col("o.customer_id"),
        F.col("o.amount"),
        F.col("c.country"),
        F.col("c.city"),
    )
)
