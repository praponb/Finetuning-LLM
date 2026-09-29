from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id", "order_id")
    .agg(
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
    .orderBy(F.col("stddev_quantity").desc())
)
