from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.month(F.col("p.launch_date")).isin([6, 8, 9]) & F.col("i.quantity").between(160, 172))
    .select(
        F.col("i.last_restock"),
        F.col("i.quantity"),
        F.col("p.brand"),
        F.col("s.supplier_name"),
        F.col("s.rating"),
    )
)
