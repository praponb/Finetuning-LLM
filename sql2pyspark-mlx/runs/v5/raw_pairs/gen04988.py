from pyspark.sql import functions as F

recent_order_items = df_order_items.filter(F.col("product_id") == 2)

summary = (
    recent_order_items
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.count("unit_price").alias("unit_price_count"),
    )
)

result = (
    summary
    .filter(F.col("unit_price_count") < 8)
    .orderBy("product_id", F.col("unit_price_count").desc())
    .limit(50)
)
