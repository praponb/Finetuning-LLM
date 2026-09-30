from pyspark.sql import functions as F

matched_keys = (
    df_order_items
    .select("order_id")
    .exceptAll(
        df_order_items
        .filter(F.col("product_id").isin([14, 36, 38]))
        .select("order_id")
    )
)

kept_rows = (
    df_order_items
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .withColumn("unit_price_tier", F.when(F.col("unit_price") >= 340, "PREMIUM").otherwise("STANDARD"))
    .cube("product_id", "unit_price_tier")
    .agg(
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.avg("unit_price").alias("avg_unit_price"),
        F.avg("unit_price").alias("avg_unit_price_2"),
        F.grouping("product_id").alias("grouping_id"),
    )
    .orderBy("unit_price_tier")
)
