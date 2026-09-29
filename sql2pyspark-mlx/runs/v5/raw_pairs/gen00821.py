from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "left")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter((F.col("oi.order_id") == 14) & F.col("oi.order_id").isin([2, 29, 30]))
    .select(
        F.col("oi.quantity"),
        F.col("p.brand"),
        F.col("o.order_date"),
        F.col("o.channel"),
    )
)
