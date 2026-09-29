from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.count("*").alias("cnt"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
    .filter(F.col("cnt") > 4)
    .orderBy(F.col("cnt").desc(), F.col("product_id").desc())
    .limit(10)
)
