from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.col("quantity") >= 246, "LARGE").otherwise("SMALL").alias("quantity_tier"),
    )
    .agg(
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)
