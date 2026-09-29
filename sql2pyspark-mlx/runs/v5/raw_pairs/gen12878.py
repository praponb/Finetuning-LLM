from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "outer")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .select(
        F.col("oi.product_id"),
        F.col("oi.unit_price"),
        F.col("p.category"),
        F.col("o.discount"),
    )
    .orderBy(F.col("unit_price").desc())
)
