from pyspark.sql import functions as F

recent_inventory = df_inventory.filter(F.col("product_id") == 5)

inventory_stats = (
    recent_inventory
    .groupBy("store_id")
    .agg(F.max("quantity").alias("max_quantity"))
)

result = (
    inventory_stats
    .select("store_id", "max_quantity")
    .orderBy(F.col("store_id").desc())
)
