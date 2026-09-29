from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("products.id") == F.col("inventory.product_id"), "left")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("inventory.store_id"), "left")
    .select(
        F.col("inventory.id"),
        F.col("inventory.last_restock"),
        F.col("inventory.quantity"),
        F.col("products.cost"),
        F.col("products.stock_qty"),
        F.col("stores.opened_date"),
        F.col("stores.region"),
        F.when(F.col("products.cost") >= 640, "High").when(F.col("products.cost") >= 120, "Medium").otherwise("Low").alias("cost_tier"),
    )
)
