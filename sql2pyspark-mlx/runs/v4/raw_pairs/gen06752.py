from pyspark.sql import functions as F

valid = (
    df_order_items
    .filter(F.col("unit_price") > 390)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("quantity") + F.col("unit_price")).alias("line_total"),
    )
)

result = (
    valid
    .groupBy(
        "product_id",
        F.when(F.col("unit_price") >= 420, "PREMIUM").otherwise("STANDARD").alias("unit_price_tier"),
    )
    .agg(
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.percentile_approx("unit_price", 0.9).alias("unit_price_p90"),
        F.round(F.avg("line_total"), 2).alias("avg_line_total"),
        F.percentile_approx("line_total", 0.95).alias("line_total_p95"),
    )
    .orderBy(F.col("line_total_p95").desc())
)
