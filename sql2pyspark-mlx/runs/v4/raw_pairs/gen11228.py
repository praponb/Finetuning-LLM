from pyspark.sql import functions as F

remaining_keys = (
    df_store_sales
    .select("product_id")
    .exceptAll(
        df_store_sales
        .filter(F.col("store_id").isin([2, 3, 6]))
        .select("product_id")
    )
)

reconciled = (
    df_store_sales
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.date_trunc("MONTH", "sale_date").alias("activity_month"),
        F.date_trunc("WEEK", "sale_date").alias("week_start"),
        F.when(F.col("revenue") >= 3800, "PREMIUM").otherwise("STANDARD").alias("revenue_tier"),
    )
    .agg(
        F.percentile_approx("revenue", [0.25, 0.5, 0.75]).alias("revenue_quartiles"),
        F.bit_and("units").alias("units_bit_and"),
        F.percentile_approx("revenue", 0.5).alias("revenue_p50"),
        F.percentile_approx("units", 0.95).alias("units_p95"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.stddev_samp("units").alias("units_stddev"),
    )
)
