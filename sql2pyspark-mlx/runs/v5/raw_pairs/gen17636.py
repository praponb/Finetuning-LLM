from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") >= 380) & F.col("quantity").between(4, 8) & (F.col("unit_price") <= 320))
    .groupBy(
        F.when(F.col("unit_price") >= 380, "high").when(F.col("unit_price") >= 160, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.sum("unit_price").alias("total_unit_price"),
        F.min("unit_price").alias("min_unit_price"),
    )
)
