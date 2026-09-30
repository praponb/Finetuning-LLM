from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([4, 10, 15, 26]) & F.col("product_id").isNotNull() & F.col("unit_price").between(110, 180))
    .groupBy("product_id")
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
    .filter(F.col("stddev_quantity") < 43)
    .orderBy(F.col("stddev_quantity").desc())
)
