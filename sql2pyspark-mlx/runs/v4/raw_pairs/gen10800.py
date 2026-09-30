from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("s.id") == F.col("i.store_id"), "left")
    .filter(F.col("s.opened_date") >= "2023-09-04")
    .groupBy(F.col("s.id"))
    .agg(F.count(F.col("i.id")).alias("inventory_count"))
    .filter(F.col("inventory_count") >= 4)
    .orderBy("inventory_count", "id")
)
