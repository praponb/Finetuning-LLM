from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "left")
    .filter((F.col("oi.order_id") == 5) | (F.size(F.col("p.tags")) > 2))
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("oi.product_id"),
        F.col("oi.order_id"),
        F.col("p.product_name"),
        F.col("p.supplier_id"),
        (F.col("oi.unit_price") - F.col("oi.quantity")).alias("line_total"),
    )
    .orderBy("supplier_id")
)
