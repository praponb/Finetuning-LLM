from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .filter(F.col("oi.product_id").isin([16, 35, 37]))
    .select(
        F.col("oi.order_id"),
        F.col("oi.quantity"),
        F.col("oi.product_id"),
        F.col("o.order_date"),
        F.col("o.channel"),
        F.col("o.status"),
    )
)
