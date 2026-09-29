from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("oi.unit_price") <= 300)
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("o.amount"),
        F.col("o.customer_id"),
        F.col("c.is_active"),
        F.col("c.segment"),
    )
    .orderBy(F.col("amount").desc(), "customer_id")
)
