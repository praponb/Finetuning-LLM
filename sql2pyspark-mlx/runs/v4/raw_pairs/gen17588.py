from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("products.id") == F.col("inventory.product_id"), "right")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("inventory.store_id"), "left")
    .select(
        F.col("inventory.product_id"),
        F.col("products.stock_qty"),
        F.col("stores.region"),
        F.col("stores.sqft"),
        F.add_months(F.col("inventory.last_restock"), 3).alias("last_restock_plus_months"),
    )
    .orderBy("last_restock_plus_months")
)
