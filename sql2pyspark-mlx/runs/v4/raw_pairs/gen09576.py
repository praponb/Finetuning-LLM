from pyspark.sql import functions as F

kept_keys = (
    df_store_sales
    .select("id", "product_id")
    .subtract(
        df_store_sales
        .filter(F.col("store_id").isin([3, 6]))
        .select("id", "product_id")
    )
)

reconciled = (
    df_store_sales
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .withColumn("units_tier", F.when(F.col("units") >= 40, "PREMIUM").otherwise("STANDARD"))
    .withColumn("revenue_tier", F.when(F.col("revenue") >= 4300, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["units_tier", "revenue_tier"], ["units_tier"]],
        "units_tier",
        "revenue_tier",
    )
    .agg(
        F.percentile_approx("revenue", [0.25, 0.5, 0.75]).alias("revenue_quartiles"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.percentile_approx("units", [0.25, 0.5, 0.75]).alias("units_quartiles"),
        F.min("revenue").alias("min_revenue"),
        F.countDistinct("id").alias("distinct_ids"),
        F.stddev_samp("units").alias("units_stddev"),
        F.grouping_id().alias("grouping_id"),
    )
)
