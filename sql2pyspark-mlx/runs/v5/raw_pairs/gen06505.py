from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("i.quantity").between(77, 143))
    .select(
        F.col("i.last_restock"),
        F.col("i.quantity"),
        F.col("s.store_name"),
        F.col("s.region"),
        F.col("s.city"),
    )
)
