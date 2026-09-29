from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "right")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("p.product_name"),
        F.col("p.brand"),
        F.col("s.country"),
        F.col("s.supplier_name"),
    )
    .orderBy("brand")
)
