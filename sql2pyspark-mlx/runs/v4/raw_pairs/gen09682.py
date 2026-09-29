from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .filter(F.col("t.stddev_quantity") > 32)
    .select(
        F.col("s.id"),
        F.col("s.region"),
        F.col("s.store_name"),
        F.col("t.quantity_count"),
        F.col("t.stddev_quantity"),
    )
    .orderBy(F.col("quantity_count").desc(), "store_name")
)
