from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 4, 6]))
    .groupBy("store_id")
    .agg(F.min("quantity").alias("min_quantity"))
)

result = (
    df_stores.alias("stores")
    .join(store_summary.alias("store_summary"), F.col("stores.id") == F.col("store_summary.store_id"), "left")
    .select(
        F.col("stores.id"),
        F.col("stores.region"),
        F.col("store_summary.min_quantity"),
    )
)
