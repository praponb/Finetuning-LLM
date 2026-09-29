from pyspark.sql import functions as F

store_stats = (
    df_inventory
    .filter(F.col("last_restock") > "2023-10-01")
    .groupBy("store_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(
        F.col("s.region"),
        F.col("s.store_name"),
        F.col("ss.avg_quantity"),
        F.col("ss.max_quantity"),
    )
    .orderBy(F.col("region").desc(), F.col("max_quantity").desc())
)
