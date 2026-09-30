from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "inner")
    .select(
        F.col("inventory.id"),
        F.col("inventory.quantity"),
        F.col("stores.store_name"),
    )
)
