from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .filter(F.col("store_id") == 2)
    .groupBy("store_id")
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)

result = (
    df_stores.alias("stores")
    .join(store_summary.alias("store_summary"), F.col("stores.id") == F.col("store_summary.store_id"), "left")
    .filter(F.col("store_summary.avg_quantity") >= 20)
    .select(
        F.col("stores.store_name"),
        F.col("stores.sqft"),
        F.col("store_summary.avg_quantity"),
    )
)
