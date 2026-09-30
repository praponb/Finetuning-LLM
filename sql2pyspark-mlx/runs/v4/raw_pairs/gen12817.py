from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.percentile_approx("quantity", 0.9).alias("quantity_p90"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
    .orderBy(F.col("store_id").desc_nulls_first())
)
