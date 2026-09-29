from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "inner")
    .filter((F.col("stores.opened_date") <= "2025-05-01") & (F.col("stores.region") == "East"))
    .select(
        F.col("inventory.product_id"),
        F.col("stores.store_name"),
        F.col("stores.region"),
    )
)
