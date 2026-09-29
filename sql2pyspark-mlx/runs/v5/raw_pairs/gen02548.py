from pyspark.sql import functions as F

base = df_inventory.filter(F.col("quantity") <= 147)

agg = (
    base
    .groupBy("store_id")
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)

result = agg.orderBy(F.col("store_id").desc())
