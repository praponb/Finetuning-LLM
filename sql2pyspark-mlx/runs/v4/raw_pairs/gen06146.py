from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(4, 6))
    .groupBy("order_id")
    .agg(F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"))
    .filter(F.col("stddev_unit_price") >= 65)
    .select("order_id", "stddev_unit_price")
)
