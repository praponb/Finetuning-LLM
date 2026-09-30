from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("o.customer_id"))
    .agg(
        F.min(F.col("oi.unit_price")).alias("min_unit_price"),
        F.avg(F.col("oi.quantity")).alias("avg_quantity"),
    )
    .orderBy(F.col("min_unit_price").desc())
)
