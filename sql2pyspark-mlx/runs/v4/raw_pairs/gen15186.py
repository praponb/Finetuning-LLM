from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .groupBy("store_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("ss.quantity_range") >= 63)
    .select(
        F.col("s.region"),
        F.col("s.opened_date"),
        F.col("ss.quantity_range"),
        F.col("ss.stddev_quantity"),
    )
    .orderBy("stddev_quantity")
)
