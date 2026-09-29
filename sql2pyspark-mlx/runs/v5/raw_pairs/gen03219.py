from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "right")
    .select(
        F.col("inventory.store_id"),
        F.col("stores.opened_date"),
        F.col("stores.region"),
    )
    .orderBy("region", "store_id")
)
