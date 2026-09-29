from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "right")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("inventory.quantity") == 99)
    .select(
        F.col("inventory.id"),
        F.col("inventory.quantity"),
        F.col("inventory.store_id"),
        F.col("products.cost"),
        F.col("products.category"),
        F.col("stores.city"),
        F.col("stores.store_name"),
    )
    .orderBy("category")
)
