from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "left")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "inner")
    .select(
        F.col("inventory.id"),
        F.col("inventory.product_id"),
        F.col("inventory.quantity"),
        F.col("products.launch_date"),
        F.col("products.cost"),
        F.col("stores.region"),
        F.col("stores.opened_date"),
    )
)
