from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") >= 208)
    .groupBy("product_id", "store_id")
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
    .filter(F.col("stddev_quantity") < 40)
)
