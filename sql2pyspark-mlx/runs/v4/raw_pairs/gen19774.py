from pyspark.sql import functions as F

store_stats = (
    df_inventory
    .filter(F.col("product_id") == 7)
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.count("*").alias("row_count"),
    )
)

result = (
    df_stores.alias("stores")
    .join(store_stats.alias("store_stats"), F.col("stores.id") == F.col("store_stats.store_id"), "inner")
    .select(
        F.col("stores.id"),
        F.col("stores.city"),
        F.col("store_stats.stddev_quantity"),
        F.col("store_stats.row_count"),
    )
)
