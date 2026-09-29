from pyspark.sql import functions as F

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select("*", (F.col("quantity") + F.col("unit_price")).alias("line_total"))
)

result = (
    filtered
    .groupBy("order_id")
    .agg(
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.count("*").alias("row_count"),
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
        F.percentile_approx("line_total", 0.5).alias("line_total_p50"),
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles_2"),
    )
    .orderBy(F.col("order_id").asc_nulls_last())
)
