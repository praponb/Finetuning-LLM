from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .filter(F.col("last_restock") < "2024-08-01")
    .groupBy("store_id")
    .agg(F.min("last_restock").alias("min_last_restock"))
)

result = (
    df_stores.alias("s")
    .join(inventory_by_store.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .select(F.col("s.id"), F.col("s.sqft"), F.col("t.min_last_restock"))
)
