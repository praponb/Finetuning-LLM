from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_inventory.alias("inventory"), F.col("inventory.store_id") == F.col("stores.id"), "left")
    .filter(F.col("stores.opened_date") >= "2024-06-01")
    .groupBy(F.col("stores.id"))
    .agg(F.count(F.col("inventory.id")).alias("inventory_count"))
    .filter(F.col("inventory_count") >= 2)
)
