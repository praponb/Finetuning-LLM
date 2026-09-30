from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter((F.col("oi.unit_price") >= 220) & F.col("o.store_id").isNull())
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("o.store_id"),
        F.col("o.shipping_fee"),
        F.col("o.status"),
    )
)
