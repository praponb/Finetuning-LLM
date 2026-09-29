from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .filter(F.month("last_restock").isin([3, 6, 7]))
    .groupBy("store_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(F.col("s.region"), F.col("ss.quantity_range"))
    .orderBy("region")
)
