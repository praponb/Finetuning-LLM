from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("i.store_id").isin([3, 6]))
    .select(
        F.col("s.store_name"),
        F.col("s.city"),
        F.col("s.sqft"),
        F.col("i.last_restock"),
        F.col("i.store_id"),
    )
    .orderBy(F.col("last_restock").desc())
)
