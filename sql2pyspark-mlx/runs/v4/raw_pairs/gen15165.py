from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("o.status"),
        F.col("o.order_date"),
        F.col("s.opened_date"),
        F.col("s.city"),
    )
    .orderBy(F.col("id").desc(), "order_date")
)
