from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.stddev_samp("unit_price").alias("unit_price_stddev"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles_2"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
    )
    .filter(F.col("unit_price_stddev") < 1000000)
)
