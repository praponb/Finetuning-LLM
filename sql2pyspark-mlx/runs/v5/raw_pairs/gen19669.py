from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.count("*").alias("num_inventory"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    df_stores.alias("stores")
    .join(store_summary.alias("store_summary"), F.col("stores.id") == F.col("store_summary.store_id"), "left")
    .select(
        F.col("stores.id"),
        F.col("stores.region"),
        F.col("store_summary.num_inventory"),
        F.col("store_summary.stddev_quantity"),
    )
    .orderBy("id", "stddev_quantity")
)
