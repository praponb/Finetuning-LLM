from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("s.id") == F.col("i.store_id"), "left")
    .filter(F.col("i.quantity") > 155)
    .groupBy(F.col("s.region"))
    .agg(F.count(F.col("i.id")).alias("inventory_count"))
    .filter(F.col("inventory_count") > 1)
    .orderBy("region")
)
