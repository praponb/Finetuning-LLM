from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("i.store_id").isin([1, 5, 6]))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("p.product_name"),
        F.col("s.supplier_name"),
        F.col("s.city"),
    )
)
