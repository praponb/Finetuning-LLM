from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "outer")
    .filter(F.size(F.col("p.tags")) > 1)
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.product_id"),
        F.col("s.region"),
        F.col("p.price"),
    )
    .orderBy(F.col("price").desc())
)
