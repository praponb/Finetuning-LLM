from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
    )
)
