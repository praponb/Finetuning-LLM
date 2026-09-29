from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 370, "PREMIUM").otherwise("STANDARD").alias("unit_price_tier"),
    )
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price_2"),
        F.percentile_approx("unit_price", 0.5).alias("unit_price_p50"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
        F.bit_or("quantity").alias("quantity_bit_or"),
    )
    .orderBy("unit_price_tier")
)
