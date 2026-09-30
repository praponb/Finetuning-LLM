from pyspark.sql import functions as F

remaining_keys = (
    df_order_items
    .select("id", "product_id")
    .exceptAll(
        df_order_items
        .filter(F.col("unit_price") <= 210)
        .select("id", "product_id")
    )
)

verified = (
    df_order_items
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("order_id")
    .agg(
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
    )
    .orderBy(F.col("order_id").asc_nulls_last())
)
