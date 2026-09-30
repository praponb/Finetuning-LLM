from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .filter(F.col("quantity") < 182)
    .groupBy("store_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.region"),
        F.col("ss.avg_quantity"),
        F.col("ss.max_quantity"),
    )
)
