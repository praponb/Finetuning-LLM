from pyspark.sql import functions as F

filtered_inventory = (
    df_inventory
    .filter(F.month("last_restock").isin([3, 6, 11]))
    .withColumn("quantity_band", F.when(F.col("quantity") >= 201, "High").when(F.col("quantity") >= 39, "Medium").otherwise("Low"))
)

inventory_stats = (
    filtered_inventory
    .groupBy("store_id")
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)

result = inventory_stats.select("store_id", "avg_quantity")
