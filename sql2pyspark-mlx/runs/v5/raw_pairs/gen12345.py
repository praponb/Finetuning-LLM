from pyspark.sql import functions as F

valid_inventory = (
    df_inventory
    .filter((F.col("last_restock") < "2024-07-13") & (F.col("quantity") < 232))
)

inventory_stats = (
    valid_inventory
    .groupBy("store_id")
    .agg(F.max("quantity").alias("max_quantity"))
)

result = (
    inventory_stats
    .filter(F.col("max_quantity") >= 80)
    .orderBy("store_id")
    .limit(5)
)
