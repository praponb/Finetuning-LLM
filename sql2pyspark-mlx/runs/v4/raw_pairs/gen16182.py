from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .groupBy("order_id")
    .agg(
        F.max("unit_price").alias("max_unit_price"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
    .orderBy(F.col("stddev_quantity").desc(), "max_unit_price")
)
