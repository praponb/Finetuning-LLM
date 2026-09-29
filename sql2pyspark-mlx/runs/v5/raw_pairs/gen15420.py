from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("i.last_restock") > "2023-10-01")
    .select(F.col("i.store_id"), F.col("s.region"))
)
