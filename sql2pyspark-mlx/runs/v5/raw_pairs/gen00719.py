from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .groupBy(
        "product_id",
        F.when(F.col("unit_price") >= 420, "high").when(F.col("unit_price") >= 180, "mid").otherwise("low").alias("unit_price_bucket"),
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.countDistinct("unit_price").alias("unique_products"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)
