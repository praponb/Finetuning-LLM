from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_inventory.alias("inventory"), F.col("inventory.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("stores.store_name").isNotNull() & (F.col("inventory.last_restock") > "2024-11-01"))
    .select(
        F.col("stores.opened_date"),
        F.col("stores.city"),
        F.col("stores.region"),
        F.col("inventory.store_id"),
    )
    .orderBy("store_id")
)
