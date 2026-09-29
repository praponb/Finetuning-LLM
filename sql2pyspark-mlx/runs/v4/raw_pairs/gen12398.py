from pyspark.sql import functions as F

recent_order_items = (
    df_order_items
    .filter(F.col("quantity").between(4, 6) & F.col("quantity").between(5, 7))
    .withColumn("unit_price_tier", F.when(F.col("unit_price") >= 390, "High").when(F.col("unit_price") >= 140, "Medium").otherwise("Low"))
)

order_items_stats = (
    recent_order_items
    .groupBy("order_id")
    .agg(F.max("unit_price").alias("max_unit_price"))
)

result = order_items_stats.orderBy("order_id")
