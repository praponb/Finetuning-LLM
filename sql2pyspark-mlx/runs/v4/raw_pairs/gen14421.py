from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") <= 110)
    .groupBy("order_id")
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
    )
    .orderBy(F.col("revenue").desc())
)
