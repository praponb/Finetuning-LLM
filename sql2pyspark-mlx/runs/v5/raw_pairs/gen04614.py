from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.filter(F.upper("store_name").like("%ALL%")).alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "left_anti")
    .select(
        F.col("inventory.id"),
        F.col("inventory.last_restock"),
        F.col("inventory.product_id"),
        F.col("inventory.store_id"),
    )
)
