from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(3, 6))
    .groupBy("order_id", "product_id")
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
    .orderBy(F.col("order_id").desc(), F.col("stddev_quantity").desc())
)
