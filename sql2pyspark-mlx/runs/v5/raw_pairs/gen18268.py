from pyspark.sql import functions as F

kept_keys = (
    df_inventory
    .select("store_id", "id")
    .intersect(
        df_inventory
        .filter(F.col("product_id").isin([1, 9]))
        .select("store_id", "id")
    )
)

kept_rows = (
    df_inventory
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .rollup("product_id", F.date_trunc("YEAR", "last_restock").alias("year_start"))
    .agg(
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("quantity").alias("min_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles_2"),
    )
)
