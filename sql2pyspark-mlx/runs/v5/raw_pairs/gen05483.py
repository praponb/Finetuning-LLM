from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 370, "high").when(F.col("unit_price") >= 110, "mid").otherwise("low").alias("unit_price_bucket"),
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.min("unit_price").alias("min_unit_price"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.max("unit_price").alias("max_unit_price"),
    )
    .filter(F.col("max_unit_price") > 170)
    .drop("max_unit_price")
    .orderBy("quantity_bucket", "unit_price_bucket")
    .limit(5)
)
