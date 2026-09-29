from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("products.id") == F.col("inventory.product_id"), "inner")
    .filter((F.size(F.col("products.tags")) >= 1) | (F.year(F.col("inventory.last_restock")) >= 2023))
    .select(
        F.col("inventory.store_id"),
        F.col("inventory.product_id"),
        F.col("inventory.last_restock"),
        F.col("products.product_name"),
        F.col("products.category"),
        F.col("products.brand"),
    )
    .orderBy("category")
)
