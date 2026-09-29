from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.col("quantity") >= 250, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
    )
    .agg(
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.count("*").alias("event_count"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles_2"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles_3"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
    .orderBy(F.col("avg_quantity").desc_nulls_last())
)
