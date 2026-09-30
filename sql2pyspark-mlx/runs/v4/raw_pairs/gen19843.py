from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id") == 29)
    .groupBy("order_id", "product_id")
    .agg(F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"))
    .orderBy(F.col("product_id").desc(), F.col("order_id").desc())
)
