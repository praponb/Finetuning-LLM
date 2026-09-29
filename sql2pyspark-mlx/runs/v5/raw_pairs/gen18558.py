from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .filter(F.col("store_id").isin([2, 4, 5, 6]))
    .groupBy("store_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(inventory_by_store.alias("ibs"), F.col("s.id") == F.col("ibs.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("ibs.total_quantity"),
        F.col("ibs.quantity_count"),
    )
    .orderBy("total_quantity", "id")
)
