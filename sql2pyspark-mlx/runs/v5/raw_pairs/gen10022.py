from pyspark.sql import functions as F

matched_keys = (
    df_order_items
    .select("id", "product_id")
    .subtract(
        df_order_items
        .filter(F.col("quantity") != 4)
        .select("id", "product_id")
    )
)

verified = (
    df_order_items
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("product_id")
    .agg(
        F.stddev_samp("unit_price").alias("unit_price_stddev"),
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
        F.min("unit_price").alias("min_unit_price"),
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("event_count").asc_nulls_first())
)
