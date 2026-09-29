from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") >= 390) & F.col("order_id").isin([5, 24, 26]) & F.col("product_id").isin([5, 7, 24, 25]))
    .groupBy(
        F.when(F.col("unit_price") >= 400, "high").when(F.col("unit_price") >= 110, "mid").otherwise("low").alias("unit_price_bucket"),
        "product_id",
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.countDistinct("quantity").alias("unique_orders"),
    )
    .orderBy(F.col("avg_quantity").desc(), "unit_price_bucket", "product_id")
    .limit(5)
)
