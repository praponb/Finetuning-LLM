from pyspark.sql import functions as F

recent_inventory = df_inventory.filter(F.col("product_id").isin([1, 3, 4, 5]))

inventory_stats = (
    recent_inventory
    .groupBy("product_id", "store_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.min("last_restock").alias("min_last_restock"),
    )
)

result = (
    inventory_stats
    .filter(F.col("max_quantity") > 180)
    .orderBy(F.col("product_id").desc(), F.col("store_id").desc())
    .limit(5)
)
