from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "left")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("stores.opened_date") >= "2025-05-01")
    .select(
        F.col("inventory.id"),
        F.col("inventory.quantity"),
        F.col("products.brand"),
        F.col("stores.region"),
        F.col("stores.city"),
    )
)
