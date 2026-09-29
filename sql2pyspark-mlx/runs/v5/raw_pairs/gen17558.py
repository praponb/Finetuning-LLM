from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .select(F.col("i.id"), F.col("i.product_id"), F.col("i.store_id"), F.col("s.city"))
    .orderBy("city", "id")
    .limit(5)
)
