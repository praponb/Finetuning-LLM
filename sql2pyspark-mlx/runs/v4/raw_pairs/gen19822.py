from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 9, "LARGE").otherwise("SMALL").alias("quantity_tier"),
        "product_id",
    )
    .agg(
        F.percentile_approx("unit_price", 0.5).alias("unit_price_p50"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.min("unit_price").alias("min_unit_price"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
    .orderBy(F.col("avg_quantity").desc())
)
