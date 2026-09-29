from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.col("quantity") >= 234, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity_2"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity_3"),
        F.stddev_samp("quantity").alias("quantity_stddev_2"),
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
    )
)
