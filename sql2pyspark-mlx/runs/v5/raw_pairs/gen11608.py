from pyspark.sql import functions as F

recent_inventory = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2]) & (F.col("quantity") < 183))
    .withColumn("quantity_filled", F.coalesce("quantity", F.lit(0)))
)

inventory_stats = (
    recent_inventory
    .groupBy("store_id")
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)

result = (
    inventory_stats
    .select("store_id", "avg_quantity")
    .orderBy(F.col("avg_quantity").desc(), F.col("store_id").desc())
    .limit(10)
)
