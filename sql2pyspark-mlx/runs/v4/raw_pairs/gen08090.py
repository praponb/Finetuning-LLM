from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id", "order_id")
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
    .orderBy(F.col("product_id").desc())
)
