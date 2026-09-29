from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("i.store_id").isin([2, 3, 4]) & F.col("i.store_id").isin([1, 4, 5]))
    .select(
        F.col("i.last_restock"),
        F.col("s.opened_date"),
        F.col("s.city"),
        F.round(F.col("i.quantity"), 0).alias("quantity_rounded"),
    )
    .orderBy(F.col("quantity_rounded").desc())
)
