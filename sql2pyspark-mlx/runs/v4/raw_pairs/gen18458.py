from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.round(F.avg("units"), 2).alias("avg_units"))
)

result = (
    df_stores.alias("stores")
    .join(store_stats.alias("store_stats"), F.col("stores.id") == F.col("store_stats.store_id"), "left")
    .select(
        F.col("stores.id"),
        F.col("stores.store_name"),
        F.col("stores.sqft"),
        F.col("store_stats.avg_units"),
    )
    .orderBy("avg_units")
)
