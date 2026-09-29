from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "right")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .groupBy(F.col("products.brand"), F.col("products.category"))
    .agg(
        (F.max(F.col("inventory.quantity")) - F.min(F.col("inventory.quantity"))).alias("quantity_range"),
    )
)
