from pyspark.sql import functions as F

filtered_inventory = df_inventory.filter(F.col("product_id").isin([6, 10]))

inventory_stats = (
    filtered_inventory
    .groupBy("store_id", "product_id")
    .agg(
        F.min("last_restock").alias("min_last_restock"),
        F.min("last_restock").alias("first_last_restock"),
    )
)

result = (
    inventory_stats
    .select("store_id", "product_id", "min_last_restock", "first_last_restock")
    .orderBy("first_last_restock", "store_id", "product_id")
    .limit(1)
)
