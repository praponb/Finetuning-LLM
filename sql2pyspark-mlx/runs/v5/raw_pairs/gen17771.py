from pyspark.sql import functions as F

src = (
    df_order_items
    .filter(F.col("product_id").isNull() & F.col("unit_price").between(230, 310) & (F.col("quantity") > 3))
)

order_items_stats = (
    src
    .groupBy(
        F.when(F.col("unit_price") >= 300, "high").when(F.col("unit_price") >= 120, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.countDistinct("quantity").alias("unique_orders"),
    )
)

result = (
    order_items_stats
    .filter(F.col("unique_orders") >= 5)
    .select("unit_price_bucket", "avg_quantity", "unique_orders")
    .orderBy(F.col("avg_quantity").desc(), "unit_price_bucket")
    .limit(1)
)
