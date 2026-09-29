from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.month(F.col("s.opened_date")).isin([2, 3, 5]) & (F.year(F.col("s.opened_date")) >= 2024))
    .select(
        F.col("s.city"),
        F.col("i.store_id"),
        F.col("i.last_restock"),
        F.col("i.quantity"),
    )
)
