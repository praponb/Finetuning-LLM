from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .filter(F.col("i.quantity").isNull())
    .select(
        F.col("i.product_id"),
        F.col("i.last_restock"),
        F.col("p.category"),
        F.col("s.sqft"),
        F.col("s.region"),
    )
    .orderBy("category", "last_restock")
)
