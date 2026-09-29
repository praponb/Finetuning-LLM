from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "left")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("oi.quantity"),
        F.col("p.price"),
        F.col("p.brand"),
        F.col("s.supplier_name"),
    )
    .orderBy("supplier_name", "price")
)
