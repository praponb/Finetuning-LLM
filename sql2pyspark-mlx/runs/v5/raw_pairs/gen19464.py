from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.min("last_restock").alias("min_last_restock"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .select(
        F.col("s.region"),
        F.col("s.store_name"),
        F.col("t.avg_quantity"),
        F.col("t.min_last_restock"),
    )
    .orderBy("avg_quantity")
)
