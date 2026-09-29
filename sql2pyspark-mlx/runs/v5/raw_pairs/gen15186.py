from pyspark.sql import functions as F

store_stats = (
    df_inventory
    .filter(F.col("store_id").isin([3, 5, 6]))
    .groupBy("store_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.min("last_restock").alias("first_last_restock"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("ss.total_quantity") >= 1270)
    .select(
        F.col("s.store_name"),
        F.col("ss.total_quantity"),
        F.col("ss.first_last_restock"),
    )
    .orderBy("store_name")
)
