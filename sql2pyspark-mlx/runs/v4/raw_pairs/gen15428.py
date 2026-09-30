from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "left")
    .join(df_products.alias("products"), F.col("products.id") == F.col("inventory.product_id"), "left")
    .filter(F.array_contains(F.col("products.tags"), "bestseller"))
    .select(
        F.col("inventory.id"),
        F.col("inventory.quantity"),
        F.col("inventory.last_restock"),
        F.col("stores.store_name"),
        F.col("products.launch_date"),
    )
)
