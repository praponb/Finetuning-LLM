from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.city"))
    .agg(
        F.count(F.col("i.quantity")).alias("quantity_count"),
        F.min(F.col("i.last_restock")).alias("first_last_restock"),
    )
    .orderBy("first_last_restock", "city")
)
