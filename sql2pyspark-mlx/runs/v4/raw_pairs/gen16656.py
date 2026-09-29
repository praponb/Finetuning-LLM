from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("store_id") == 4) & (F.col("quantity") <= 159))
    .groupBy("store_id")
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
)
