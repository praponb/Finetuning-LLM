from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("s.id") == F.col("i.store_id"), "left")
    .filter(F.col("s.sqft").between(8129, 10787))
    .groupBy(F.col("s.id"), F.col("s.store_name"))
    .agg(F.count(F.col("i.id")).alias("inventory_count"))
    .orderBy(F.col("id").desc())
)
