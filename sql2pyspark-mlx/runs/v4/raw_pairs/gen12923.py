from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("o.order_date"),
        F.col("o.channel"),
        F.col("c.age"),
    )
    .orderBy(F.col("order_date").desc(), "id")
    .limit(10)
)
