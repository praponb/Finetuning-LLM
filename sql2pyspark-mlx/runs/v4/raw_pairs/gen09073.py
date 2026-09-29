from pyspark.sql import functions as F

kept_keys = (
    df_store_sales
    .select("product_id")
    .intersectAll(
        df_store_sales
        .filter(F.month("sale_date").isin([2, 7, 9]))
        .select("product_id")
    )
)

verified = (
    df_store_sales
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

kept_keys_2 = (
    verified
    .select("store_id")
    .intersect(
        df_store_sales
        .filter(F.col("units") <= 36)
        .select("store_id")
    )
)

reconciled = (
    verified
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter(F.col("revenue").isNull())
    .select("*", F.round((F.col("revenue") * 0.85), 2).alias("revenue_scaled"))
)

grouped = (
    cleaned
    .withColumn("units_tier", F.when(F.col("units") >= 43, "HIGH").otherwise("LOW"))
    .withColumn("revenue_tier", F.when(F.col("revenue") >= 3400, "LARGE").otherwise("SMALL"))
    .rollup("units_tier", "store_id", "revenue_tier")
    .agg(
        F.sum("units").alias("total_units"),
        F.corr("revenue", "units").alias("corr_revenue_units"),
        F.count("*").alias("row_count"),
        F.percentile_approx("revenue", 0.95).alias("revenue_p95"),
        F.grouping_id().alias("grouping_id"),
    )
)

result = (
    grouped
    .select(
        "units_tier",
        "store_id",
        "revenue_tier",
        "total_units",
        "corr_revenue_units",
        "revenue_p95",
        "grouping_id",
    )
    .orderBy(
        F.col("grouping_id").desc_nulls_first(),
        F.col("corr_revenue_units").desc(),
    )
)
