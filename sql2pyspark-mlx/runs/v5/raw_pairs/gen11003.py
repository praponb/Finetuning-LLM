from pyspark.sql import functions as F

matched_keys = (
    df_inventory
    .select("store_id")
    .exceptAll(
        df_inventory
        .filter(F.col("last_restock") < "2025-07-01")
        .select("store_id")
    )
)

kept_rows = (
    df_inventory
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 248, "LARGE").otherwise("SMALL"))
    .rollup("store_id", "quantity_tier")
    .agg(
        F.percentile_approx("quantity", 0.9).alias("quantity_p90"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("quantity_p90").desc())
)
