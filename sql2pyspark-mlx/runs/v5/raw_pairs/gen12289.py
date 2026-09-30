from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .filter(F.col("i.last_restock").between("2023-12-01", "2025-07-04"))
    .select(
        F.col("i.product_id"),
        F.col("p.supplier_id"),
        F.col("p.category"),
        F.upper(F.trim(F.col("p.product_name"))).alias("product_name_normalized"),
    )
    .orderBy("product_name_normalized", "supplier_id")
)
