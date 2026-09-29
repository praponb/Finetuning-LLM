from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "right")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("p.launch_date"),
        F.col("s.supplier_name"),
        F.col("s.country"),
    )
    .orderBy("launch_date")
)
