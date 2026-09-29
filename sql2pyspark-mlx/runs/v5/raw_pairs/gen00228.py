from pyspark.sql import functions as F

remaining_keys = (
    df_order_items
    .select("id", "order_id")
    .subtract(
        df_order_items
        .filter(F.col("quantity") == 7)
        .select("id", "order_id")
    )
)

reconciled = (
    df_order_items
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("unit_price") >= 340, "HIGH").otherwise("LOW").alias("unit_price_tier"),
    )
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.sum("unit_price").alias("total_unit_price"),
    )
    .orderBy(F.col("num_rows").desc_nulls_last())
)
