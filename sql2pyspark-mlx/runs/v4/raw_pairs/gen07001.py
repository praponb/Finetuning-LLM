from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("i.last_restock").between("2023-10-01", "2024-01-01"))
    .select(
        F.col("i.product_id"),
        F.col("i.store_id"),
        F.col("p.launch_date"),
        F.col("p.product_name"),
        F.col("s.country"),
        F.col("s.supplier_name"),
        F.round(F.col("p.price"), 0).alias("price_rounded"),
    )
)
