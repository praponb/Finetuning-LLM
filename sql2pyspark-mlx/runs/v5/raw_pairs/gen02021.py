from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.region"))
    .agg(F.max(F.col("i.quantity")).alias("max_quantity"))
    .orderBy(F.col("max_quantity").desc())
)
