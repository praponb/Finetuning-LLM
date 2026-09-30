from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_inventory.alias("inventory"), F.col("products.id") == F.col("inventory.product_id"), "inner")
    .select(
        F.col("products.category"),
        F.col("inventory.last_restock"),
        F.col("inventory.store_id"),
    )
    .orderBy("last_restock")
)
