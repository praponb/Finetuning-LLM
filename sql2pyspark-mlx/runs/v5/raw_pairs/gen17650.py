from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.count("*").alias("row_count"),
        F.count("quantity").alias("quantity_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(inventory_by_store.alias("ibs"), F.col("s.id") == F.col("ibs.store_id"), "left")
    .filter(F.col("ibs.row_count") >= 3)
    .select(F.col("s.region"), F.col("ibs.row_count"), F.col("ibs.quantity_count"))
    .orderBy("region", F.col("row_count").desc())
)
