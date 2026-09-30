from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("i.quantity"),
        F.col("p.supplier_id"),
        F.col("p.price"),
        F.col("s.sqft"),
        F.lower(F.col("p.category")).alias("category_lower"),
    )
)
