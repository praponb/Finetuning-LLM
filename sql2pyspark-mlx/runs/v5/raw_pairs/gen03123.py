from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_inventory.alias("inventory"), F.col("stores.id") == F.col("inventory.store_id"), "left")
    .select(
        F.col("stores.region"),
        F.col("stores.store_name"),
        F.col("stores.opened_date"),
        F.col("inventory.last_restock"),
        F.col("inventory.quantity"),
        F.col("inventory.store_id"),
    )
    .orderBy("quantity")
)
