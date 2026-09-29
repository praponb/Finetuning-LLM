from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.max("quantity").alias("max_quantity"),
    )
    .orderBy(F.col("max_quantity").desc(), "order_id")
    .limit(50)
)
