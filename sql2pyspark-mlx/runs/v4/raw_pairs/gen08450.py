from pyspark.sql import functions as F

filtered_order_items = (
    df_order_items
    .filter(F.col("quantity") != 3)
    .withColumn("line_total", F.round((F.col("unit_price") / F.col("quantity")), 2))
)

summary = (
    filtered_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 320, "high").when(F.col("unit_price") >= 50, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.countDistinct("line_total").alias("unique_orders"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = summary
