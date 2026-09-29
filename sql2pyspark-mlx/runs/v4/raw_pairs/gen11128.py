from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") <= 121) & (F.col("product_id").isin([1, 8, 9]) | F.col("quantity").between(237, 240)))
    .groupBy("store_id", "product_id")
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
    .filter(F.col("stddev_quantity") > 64)
)
