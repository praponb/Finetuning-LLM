from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .filter((F.col("s.city") == "Singapore") | (F.col("s.region") == "South"))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("i.store_id"),
        F.col("i.last_restock"),
        F.col("s.region"),
        F.col("s.store_name"),
    )
    .orderBy(F.col("quantity").desc(), "last_restock")
)
