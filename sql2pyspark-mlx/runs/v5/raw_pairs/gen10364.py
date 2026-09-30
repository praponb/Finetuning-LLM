from pyspark.sql import functions as F

recent_order_items = df_order_items.filter(F.col("unit_price") >= 270)

summary = (
    recent_order_items
    .groupBy("order_id", "product_id")
    .agg(F.round(F.avg("unit_price"), 2).alias("avg_unit_price"))
)

result = (
    summary
    .select("order_id", "product_id", "avg_unit_price")
    .orderBy(F.col("order_id").desc(), F.col("avg_unit_price").desc())
)
