from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("i.quantity") == 123)
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("p.launch_date"),
        F.col("s.city"),
    )
)
