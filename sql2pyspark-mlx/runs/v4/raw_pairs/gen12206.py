from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter((F.col("oi.product_id") == 3) | (F.col("oi.quantity") > 6))
    .select(
        F.col("oi.unit_price"),
        F.col("o.status"),
        F.col("p.cost"),
        F.col("p.category"),
    )
    .orderBy("cost")
)
