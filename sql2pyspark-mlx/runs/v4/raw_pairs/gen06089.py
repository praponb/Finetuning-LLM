from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("i.product_id").isin([2, 7]))
    .groupBy(F.col("s.city"))
    .agg(
        F.count(F.col("i.id")).alias("inventory_count"),
        F.max(F.col("i.last_restock")).alias("latest_last_restock"),
    )
)
