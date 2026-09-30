from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(87, 234) & (F.col("quantity") == 196) & F.col("product_id").isin([7, 10]))
    .groupBy("product_id")
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
    .filter(F.col("stddev_quantity") < 29)
)
