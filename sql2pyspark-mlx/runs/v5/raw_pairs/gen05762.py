from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .filter(F.col("store_id").isin([4, 6]))
    .groupBy("store_id")
    .agg(F.min("quantity").alias("min_quantity"))
)

result = (
    df_stores.alias("s")
    .join(inventory_by_store.alias("ibs"), F.col("s.id") == F.col("ibs.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.opened_date"),
        F.col("ibs.min_quantity"),
    )
    .orderBy(F.col("city").desc())
)
