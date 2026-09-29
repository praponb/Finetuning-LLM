from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("p.supplier_id").isNotNull() | (F.size(F.col("p.tags")) > 2))
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("p.product_name"),
        F.col("o.store_id"),
        F.col("o.amount"),
    )
)
