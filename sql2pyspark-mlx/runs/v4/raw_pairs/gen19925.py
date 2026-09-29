from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .filter(F.col("last_restock") < "2024-02-07")
    .groupBy("store_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("ss.quantity_range") >= 36)
    .select(
        F.col("s.id"),
        F.col("s.sqft"),
        F.col("s.store_name"),
        F.col("ss.avg_quantity"),
        F.col("ss.quantity_range"),
    )
)
