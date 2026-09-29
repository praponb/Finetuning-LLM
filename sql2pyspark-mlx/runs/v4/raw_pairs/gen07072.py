from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(~(F.month("last_restock").isin([2, 4, 5]) & F.col("store_id").isin([1, 4, 5])))
    .withColumn("quantity_whole", F.floor("quantity"))
)

inventory_stats = (
    base
    .groupBy("product_id")
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)

result = (
    inventory_stats
    .select("product_id", "avg_quantity")
    .orderBy(F.col("product_id").desc())
)
