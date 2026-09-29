from pyspark.sql import functions as F

result = (
    df_order_items
    .cube(
        F.when(F.col("quantity") >= 7, "LARGE").otherwise("SMALL").alias("quantity_tier"),
        "product_id",
        "order_id",
    )
    .agg(
        F.stddev_samp("unit_price").alias("unit_price_stddev"),
        F.percentile_approx("unit_price", 0.5).alias("unit_price_p50"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
    .filter(F.col("unit_price_p50") >= 0)
    .orderBy(F.col("quantity_tier").asc_nulls_last())
)
