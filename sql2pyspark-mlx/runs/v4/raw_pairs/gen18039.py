from pyspark.sql import functions as F

kept_keys = (
    df_order_items
    .select("order_id", "id")
    .subtract(
        df_order_items
        .filter(F.col("product_id").isNotNull())
        .select("order_id", "id")
    )
)

reconciled = (
    df_order_items
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("unit_price") >= 430, "HIGH").otherwise("LOW").alias("unit_price_tier"),
    )
    .agg(
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles_2"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
    )
)
