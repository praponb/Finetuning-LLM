from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("inventory.store_id"), "left")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .filter(F.col("inventory.quantity") > 114)
    .select(
        F.col("inventory.id"),
        F.col("inventory.last_restock"),
        F.col("inventory.product_id"),
        F.col("stores.opened_date"),
        F.col("products.stock_qty"),
        F.date_sub(F.col("inventory.last_restock"), 83).alias("last_restock_minus_83d"),
    )
    .orderBy("last_restock_minus_83d")
)
