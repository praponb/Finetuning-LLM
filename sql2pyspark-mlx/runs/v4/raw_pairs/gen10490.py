from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "right")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("i.store_id").isin([1, 5, 6]))
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("p.category"),
        F.col("p.cost"),
        F.col("s.city"),
        F.col("s.country"),
    )
    .orderBy("city")
)
