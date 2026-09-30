from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNull() & (F.col("unit_price") >= 310))
    .groupBy("product_id")
    .agg(F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"))
    .filter(F.col("stddev_unit_price") > 35)
    .orderBy(F.col("product_id").desc())
    .limit(20)
)
