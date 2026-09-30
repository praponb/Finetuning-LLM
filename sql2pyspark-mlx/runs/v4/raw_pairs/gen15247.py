from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "right")
    .filter(F.col("oi.product_id") == 5)
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("o.store_id"),
        F.col("o.channel"),
        F.col("s.region"),
        (F.col("o.amount") - F.col("o.discount")).alias("amount_minus_discount"),
    )
)
