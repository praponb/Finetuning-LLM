from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("products.id") == F.col("inventory.product_id"), "inner")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("inventory.store_id"), "left")
    .filter(F.col("products.category") == "Books")
    .select(
        F.col("inventory.id"),
        F.col("inventory.last_restock"),
        F.col("inventory.store_id"),
        F.col("products.launch_date"),
        F.col("products.product_name"),
        F.col("stores.sqft"),
    )
)
