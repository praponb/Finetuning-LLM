from pyspark.sql import functions as F

result = (
    df_order_items
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.count("*").alias("row_count"),
    )
    .orderBy("row_count", F.col("stddev_quantity").desc())
)
