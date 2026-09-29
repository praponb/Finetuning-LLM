from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.launch_date").between("2023-04-01", "2024-03-01") & (F.col("oi.unit_price") >= 150))
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("oi.unit_price"),
        F.col("p.brand"),
        F.col("p.product_name"),
        F.col("s.country"),
    )
)
