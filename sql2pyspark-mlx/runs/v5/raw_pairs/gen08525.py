from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "left")
    .filter(F.col("s.store_name").like("D%"))
    .select(F.col("i.id"), F.col("i.store_id"), F.col("s.store_name"))
)
