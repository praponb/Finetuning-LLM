from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "right")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "inner")
    .groupBy(F.col("products.category"))
    .agg(
        F.countDistinct(F.col("inventory.quantity")).alias("unique_products"),
        F.count(F.col("inventory.quantity")).alias("quantity_count"),
    )
    .orderBy(F.col("unique_products").desc())
)
