from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 320, "LARGE").otherwise("SMALL").alias("unit_price_tier"),
    )
    .agg(
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
        F.percentile_approx("unit_price", 0.9).alias("unit_price_p90"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
)
