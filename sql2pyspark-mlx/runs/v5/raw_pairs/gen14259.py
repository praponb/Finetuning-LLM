from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("oi.product_id").isin([22, 27, 30, 37]))
    .select(
        F.col("o.store_id"),
        F.col("o.channel"),
        F.col("oi.quantity"),
        F.col("oi.unit_price"),
        F.col("oi.order_id"),
    )
)
