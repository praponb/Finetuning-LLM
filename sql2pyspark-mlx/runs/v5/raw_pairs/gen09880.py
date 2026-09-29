from pyspark.sql import functions as F

recent_order_items = (
    df_order_items
    .filter((F.col("unit_price") <= 220) & F.col("unit_price").between(160, 370))
)

unit_price_bucket_summary = (
    recent_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 430, "high").when(F.col("unit_price") >= 80, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"))
)

result = (
    unit_price_bucket_summary
    .select("unit_price_bucket", "stddev_unit_price")
    .orderBy(F.col("unit_price_bucket").desc(), F.col("stddev_unit_price").desc())
)
