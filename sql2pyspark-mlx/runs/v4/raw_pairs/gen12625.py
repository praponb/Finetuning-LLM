from pyspark.sql import functions as F

kept_keys = (
    df_inventory
    .select("store_id", "id")
    .intersectAll(
        df_inventory
        .filter(F.col("product_id").isin([1, 3, 4, 10]))
        .select("store_id", "id")
    )
)

reconciled = (
    df_inventory
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.date_trunc("QUARTER", "last_restock").alias("quarter_start"),
        "product_id",
        F.when(F.col("quantity") >= 266, "HIGH").otherwise("LOW").alias("quantity_tier"),
    )
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.avg("quantity").alias("avg_quantity"),
        F.count("*").alias("row_count"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.count("*").alias("event_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
