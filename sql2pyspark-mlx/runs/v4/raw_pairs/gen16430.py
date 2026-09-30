from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .filter(F.col("last_restock") < "2024-03-01")
    .groupBy("store_id")
    .agg(F.min("last_restock").alias("min_last_restock"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .select(F.col("s.id"), F.col("s.city"), F.col("t.min_last_restock"))
)
