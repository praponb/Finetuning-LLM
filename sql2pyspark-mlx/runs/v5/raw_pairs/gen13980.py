from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.store_name").isNull())
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("s.opened_date"),
        F.col("s.store_name"),
        F.col("s.region"),
    )
)
