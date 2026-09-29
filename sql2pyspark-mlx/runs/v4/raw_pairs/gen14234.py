from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("stores.region").isNull())
    .select(
        F.col("inventory.id"),
        F.col("inventory.last_restock"),
        F.col("inventory.product_id"),
        F.col("stores.sqft"),
    )
)
