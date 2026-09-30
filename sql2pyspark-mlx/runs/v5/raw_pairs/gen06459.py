from pyspark.sql import functions as F

remaining_keys = (
    df_order_items
    .select("order_id", "product_id")
    .intersect(
        df_order_items
        .filter(F.col("order_id").isin([8, 9, 18, 21]))
        .select("order_id", "product_id")
    )
)

kept_rows = (
    df_order_items
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.when(F.col("quantity") >= 7, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
    )
    .agg(
        F.percentile_approx("unit_price", 0.9).alias("unit_price_p90"),
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
    )
    .filter(F.col("quantity_p50") > 0)
    .orderBy(F.col("num_rows").asc_nulls_last())
)
