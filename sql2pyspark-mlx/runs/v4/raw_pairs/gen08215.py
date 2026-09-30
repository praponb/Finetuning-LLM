from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("p.stock_qty") == 288)
    .select(
        F.col("oi.unit_price"),
        F.col("oi.quantity"),
        F.col("p.price"),
        F.col("p.launch_date"),
        F.col("o.store_id"),
        F.col("o.amount"),
    )
)
