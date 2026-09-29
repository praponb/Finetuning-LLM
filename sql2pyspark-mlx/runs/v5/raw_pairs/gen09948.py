from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "left")
    .groupBy(F.col("s.city"))
    .agg(F.min(F.col("i.last_restock")).alias("first_last_restock"))
    .orderBy("city")
)
