from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "left")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("i.store_id").isin([1, 3, 4, 6]) | F.lower(F.col("s.supplier_name")).like("%uro%"))
    .select(
        F.col("i.store_id"),
        F.col("i.product_id"),
        F.col("p.brand"),
        F.col("s.rating"),
    )
)
