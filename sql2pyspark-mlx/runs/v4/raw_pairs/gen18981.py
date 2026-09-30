from pyspark.sql import functions as F

store_stats = (
    df_inventory
    .groupBy("store_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.count("quantity").alias("quantity_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("ss.quantity_range") > 62)
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("ss.quantity_range"),
        F.col("ss.quantity_count"),
    )
)
