from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "outer")
    .filter(F.col("oi.product_id").isin([6, 9, 14, 37]))
    .select(
        F.col("oi.quantity"),
        F.col("o.amount"),
        F.col("o.store_id"),
        F.col("o.discount"),
        (F.col("oi.unit_price") + F.col("oi.quantity")).alias("line_total"),
    )
)
