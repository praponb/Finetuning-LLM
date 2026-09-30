from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.store_name").like("O%") | (F.col("s.city") == "London"))
    .select(
        F.col("i.last_restock"),
        F.col("s.opened_date"),
        F.col("s.city"),
        F.col("s.store_name"),
    )
    .orderBy("city", F.col("opened_date").desc())
)
