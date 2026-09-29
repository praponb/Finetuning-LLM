from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 8, "HIGH").otherwise("LOW").alias("quantity_tier"),
    )
    .agg(
        F.percentile_approx("unit_price", 0.9).alias("unit_price_p90"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.count("*").alias("cnt"),
        F.corr("unit_price", "quantity").alias("corr_unit_price_quantity"),
        F.percentile_approx("unit_price", 0.5).alias("unit_price_p50"),
    )
)
