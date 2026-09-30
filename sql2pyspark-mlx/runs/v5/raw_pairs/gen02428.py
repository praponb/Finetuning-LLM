from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([9, 21]))
    .groupBy("order_id")
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
    .filter(F.col("stddev_quantity") < 20)
    .orderBy(F.col("stddev_quantity").desc(), F.col("order_id").desc())
)
