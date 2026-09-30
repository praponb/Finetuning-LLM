from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") < 7) & (F.col("quantity") == 7) & (F.col("product_id") == 38))
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.avg("unit_price").alias("avg_unit_price"),
        F.min("quantity").alias("min_quantity"),
    )
    .filter(F.col("stddev_quantity") > 41)
    .orderBy(F.col("avg_unit_price").desc())
)
