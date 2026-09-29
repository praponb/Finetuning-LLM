from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .filter(F.col("store_id") == 5)
    .groupBy("store_id")
    .agg(
        F.count("*").alias("row_count"),
        F.max("last_restock").alias("last_last_restock"),
    )
)

result = (
    df_stores.alias("s")
    .join(inventory_by_store.alias("ibs"), F.col("s.id") == F.col("ibs.store_id"), "left")
    .filter(F.col("ibs.row_count") >= 8)
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("ibs.row_count"),
        F.col("ibs.last_last_restock"),
    )
)
