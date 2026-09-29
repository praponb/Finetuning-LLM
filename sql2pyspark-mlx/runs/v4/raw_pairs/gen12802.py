from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("s.region"))
    .agg(F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"))
    .filter(F.col("stddev_quantity") >= 64)
    .orderBy(F.col("region").desc())
    .limit(5)
)
