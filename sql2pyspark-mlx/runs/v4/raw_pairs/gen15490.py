from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.stddev_samp("unit_price").alias("unit_price_stddev"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("unit_price_stddev").desc())
)
