from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.stddev_samp("unit_price").alias("unit_price_stddev"),
    )
    .orderBy(F.col("product_id").desc_nulls_last())
)
