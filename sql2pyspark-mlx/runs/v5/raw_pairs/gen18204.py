from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.rating") < 3.1)
    .select(
        F.col("i.product_id"),
        F.col("i.store_id"),
        F.col("p.cost"),
        F.col("p.price"),
        F.col("s.rating"),
        F.date_format(F.col("i.last_restock"), "yyyy-MM").alias("last_restock_label"),
    )
    .orderBy(F.col("price").desc())
)
