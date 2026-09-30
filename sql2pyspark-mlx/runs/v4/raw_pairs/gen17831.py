from pyspark.sql import functions as F

recent_inventory = (
    df_inventory
    .filter(~((F.col("quantity") == 172) | F.col("store_id").isin([1, 3, 4, 5])))
)

inventory_stats = (
    recent_inventory
    .groupBy("product_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = inventory_stats.select("product_id", "avg_quantity")
