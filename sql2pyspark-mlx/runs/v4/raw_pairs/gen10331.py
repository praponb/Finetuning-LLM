from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.supplier_name").like("%Lion%") & (F.length(F.col("s.supplier_name")) < 13))
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.product_id"),
        F.col("p.cost"),
        F.col("p.brand"),
        F.col("s.rating"),
        F.col("s.city"),
    )
    .orderBy(F.col("rating").desc())
)
