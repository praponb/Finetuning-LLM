from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(4, 5) & F.col("quantity").between(4, 6))
    .groupBy("order_id")
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.count("unit_price").alias("unit_price_count"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)
