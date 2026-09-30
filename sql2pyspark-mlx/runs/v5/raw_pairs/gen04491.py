from pyspark.sql import functions as F

result = (
    df_order_items
    .cube("order_id", "product_id")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.count("*").alias("num_order_items"),
    )
    .orderBy(F.col("cnt").desc(), F.col("num_order_items").desc())
)
