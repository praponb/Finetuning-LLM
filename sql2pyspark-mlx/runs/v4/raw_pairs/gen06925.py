from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.filter(F.col("last_restock") >= "2025-09-01").alias("i"), F.col("s.id") == F.col("i.store_id"), "left_anti")
    .select(F.col("s.id"), F.col("s.sqft"), F.col("s.store_name"))
)
