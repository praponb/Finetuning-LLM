from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .filter(F.col("i.store_id").isin([1, 4, 5, 6]))
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.product_id"),
        F.col("i.quantity"),
        F.col("s.city"),
        F.year(F.col("s.opened_date")).alias("opened_date_year"),
    )
)
