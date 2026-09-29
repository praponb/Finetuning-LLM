from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 320, "LARGE").otherwise("SMALL").alias("unit_price_tier"),
    )
    .agg(
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles_2"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.avg("quantity").alias("avg_quantity_2"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
    )
    .filter(F.col("avg_quantity") > 0)
    .orderBy(F.col("avg_quantity_2").desc())
)
