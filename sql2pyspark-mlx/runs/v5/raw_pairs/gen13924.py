from pyspark.sql import functions as F

recent_order_items = (
    df_order_items
    .filter((F.col("quantity") <= 6) & F.col("product_id").isin([15, 23, 24]) & F.col("product_id").isin([13, 21, 37]))
    .withColumn("line_total", (F.col("quantity") + F.col("unit_price")))
)

summary = (
    recent_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 320, "high").when(F.col("unit_price") >= 180, "mid").otherwise("low").alias("unit_price_bucket"),
        "product_id",
    )
    .agg(
        F.count("line_total").alias("line_total_count"),
        F.avg("unit_price").alias("avg_unit_price"),
    )
)

result = (
    summary
    .filter(F.col("avg_unit_price") < 120)
    .orderBy(F.col("product_id").desc())
)
