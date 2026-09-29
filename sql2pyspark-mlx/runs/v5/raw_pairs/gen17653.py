from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left")
    .select(
        F.col("o.order_date"),
        F.col("oi.unit_price"),
        F.col("oi.quantity"),
        F.col("oi.product_id"),
        F.date_sub(F.col("o.order_date"), 13).alias("order_date_minus_13d"),
    )
)
