from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .select(F.col("i.id"), F.col("i.quantity"), F.col("s.store_name"))
    .orderBy(F.col("quantity").desc(), "id")
    .limit(1)
)
