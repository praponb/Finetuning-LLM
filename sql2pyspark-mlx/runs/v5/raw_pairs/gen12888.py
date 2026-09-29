from pyspark.sql import functions as F

filtered_inventory = df_inventory.filter(F.col("product_id").isin([3, 4, 8]))

agg = (
    filtered_inventory
    .groupBy("product_id", "store_id")
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)

result = (
    agg
    .filter(F.col("avg_quantity") > 55)
    .orderBy(F.col("product_id").desc(), F.col("avg_quantity").desc())
)
