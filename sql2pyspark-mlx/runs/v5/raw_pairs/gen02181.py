from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .groupBy("store_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .select(F.col("s.id"), F.col("s.city"), F.col("t.quantity_range"))
)
