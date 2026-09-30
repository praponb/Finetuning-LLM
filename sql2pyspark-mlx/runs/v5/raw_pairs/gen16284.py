from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id") == 22)
    .agg(
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
    .orderBy(F.col("avg_unit_price").desc())
)
