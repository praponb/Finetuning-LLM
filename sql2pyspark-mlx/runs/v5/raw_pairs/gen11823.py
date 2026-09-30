from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_inventory.alias("inventory"), F.col("inventory.store_id") == F.col("stores.id"), "left")
    .filter(F.col("inventory.quantity") == 165)
    .groupBy(F.col("stores.id"), F.col("stores.city"))
    .agg(
        F.count(F.col("inventory.id")).alias("inventory_count"),
        F.max(F.col("inventory.last_restock")).alias("last_last_restock"),
        F.min(F.col("inventory.quantity")).alias("min_quantity"),
    )
    .orderBy(F.col("id").desc())
)
