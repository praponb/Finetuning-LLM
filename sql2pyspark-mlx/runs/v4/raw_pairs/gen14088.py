from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(100, 170))
    .groupBy(
        F.when(F.col("unit_price") >= 420, "high").when(F.col("unit_price") >= 140, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.sum("unit_price").alias("total_unit_price"),
    )
    .orderBy(F.col("stddev_unit_price").desc(), "unit_price_bucket")
    .limit(20)
)
