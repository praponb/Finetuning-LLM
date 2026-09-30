from pyspark.sql import functions as F

src = (
    df_order_items
    .filter((F.col("unit_price") <= 230) & (F.col("order_id") == 32) & F.col("quantity").between(7, 8))
    .withColumn("unit_price_whole", F.ceil("unit_price"))
)

order_items_stats = (
    src
    .groupBy(
        F.when(F.col("unit_price") >= 440, "high").when(F.col("unit_price") >= 140, "mid").otherwise("low").alias("unit_price_bucket"),
        "order_id",
    )
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.min("quantity").alias("min_quantity"),
    )
)

result = (
    order_items_stats
    .filter(F.col("min_quantity") < 3.9)
    .select("unit_price_bucket", "order_id", "avg_unit_price", "min_quantity")
    .orderBy(F.col("unit_price_bucket").desc())
)
