from pyspark.sql import functions as F

remaining_keys = (
    df_order_items
    .select("order_id")
    .intersectAll(
        df_order_items
        .filter(F.col("order_id").isin([38, 39]))
        .select("order_id")
    )
)

kept_rows = (
    df_order_items
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        "order_id",
        F.when(F.col("quantity") >= 7, "LARGE").otherwise("SMALL").alias("quantity_tier"),
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.stddev_samp("unit_price").alias("unit_price_stddev"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
    .orderBy(F.col("quantity_tier").asc_nulls_last())
)
