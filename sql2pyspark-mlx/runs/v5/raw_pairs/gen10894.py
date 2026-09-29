from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.product_name").like("Yo%") | (F.col("s.city") == "Tokyo"))
    .select(
        F.col("i.store_id"),
        F.col("p.supplier_id"),
        F.col("p.launch_date"),
        F.col("s.city"),
        F.col("s.rating"),
    )
    .orderBy("supplier_id")
)
