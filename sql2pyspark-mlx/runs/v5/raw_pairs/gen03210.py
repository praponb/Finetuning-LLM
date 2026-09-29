from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.count("unit_price").alias("unit_price_count"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
    )
    .filter(F.col("stddev_quantity") >= 26)
)
