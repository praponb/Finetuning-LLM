from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "left")
    .filter((F.year(F.col("stores.opened_date")) == 2023) & F.col("inventory.store_id").isin([1, 5, 6]))
    .select(
        F.col("inventory.id"),
        F.col("inventory.last_restock"),
        F.col("inventory.quantity"),
        F.col("stores.city"),
        F.col("stores.opened_date"),
    )
    .orderBy("last_restock")
)
