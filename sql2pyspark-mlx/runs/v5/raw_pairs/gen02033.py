from pyspark.sql import functions as F

recent_order_items = df_order_items.filter(F.col("quantity").isNull())

summary = (
    recent_order_items
    .groupBy("product_id")
    .agg(F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"))
)

result = summary.select("product_id", "stddev_unit_price")
