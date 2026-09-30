from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("p.brand") == "Stark")
    .select(
        F.col("oi.unit_price"),
        F.col("p.product_name"),
        F.col("p.supplier_id"),
        F.col("s.rating"),
        F.col("s.country"),
    )
)
