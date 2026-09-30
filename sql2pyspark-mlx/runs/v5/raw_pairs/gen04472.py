from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("s.id") == F.col("i.store_id"), "left")
    .groupBy(F.col("s.id"), F.col("s.region"))
    .agg(
        F.count(F.col("i.id")).alias("inventory_count"),
        F.min(F.col("i.last_restock")).alias("min_last_restock"),
    )
    .orderBy("min_last_restock")
)
