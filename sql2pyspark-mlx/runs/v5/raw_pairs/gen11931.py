from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id", "order_id")
    .agg(F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"))
    .filter(F.col("stddev_unit_price") >= 64)
)
