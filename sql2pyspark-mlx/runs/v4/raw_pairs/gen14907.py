from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.max("last_restock").alias("latest_last_restock"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    df_stores.alias("s")
    .join(inventory_by_store.alias("ibs"), F.col("s.id") == F.col("ibs.store_id"), "inner")
    .select(
        F.col("s.region"),
        F.col("ibs.latest_last_restock"),
        F.col("ibs.stddev_quantity"),
    )
)
