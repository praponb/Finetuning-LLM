from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 7, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity_2"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity_3"),
    )
    .orderBy(F.col("avg_quantity_3").asc_nulls_last())
)
