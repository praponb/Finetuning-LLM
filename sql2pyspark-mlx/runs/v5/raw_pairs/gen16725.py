from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 310, "LARGE").otherwise("SMALL").alias("unit_price_tier"),
    )
    .agg(
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.bit_and("quantity").alias("quantity_bit_and"),
    )
)
