from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "right")
    .filter(F.col("i.store_id").isin([1, 3, 4, 6]) & (F.col("i.last_restock") > "2024-04-01"))
    .select(
        F.col("i.quantity"),
        F.col("i.store_id"),
        F.col("i.last_restock"),
        F.col("s.region"),
        F.col("s.store_name"),
    )
    .orderBy("region")
)
