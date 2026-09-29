from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("oi.unit_price").between(110, 330) & F.col("oi.order_id").isin([33, 38]))
    .select(
        F.col("oi.order_id"),
        F.col("o.status"),
        F.col("o.customer_id"),
        F.col("s.region"),
    )
    .orderBy("status", F.col("order_id").desc())
)
