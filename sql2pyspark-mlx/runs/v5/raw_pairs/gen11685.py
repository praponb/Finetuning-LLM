from pyspark.sql import functions as F

kept_keys = (
    df_store_sales
    .select("store_id", "product_id")
    .intersect(
        df_store_sales
        .filter(F.col("units").isNotNull())
        .select("store_id", "product_id")
    )
)

verified = (
    df_store_sales
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.date_trunc("WEEK", "sale_date").alias("week_start"),
        F.when(F.col("units") >= 32, "LARGE").otherwise("SMALL").alias("units_tier"),
    )
    .agg(
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.count("*").alias("cnt"),
        F.corr("units", "revenue").alias("corr_units_revenue"),
        F.percentile_approx("units", [0.25, 0.5, 0.75]).alias("units_quartiles"),
        F.min("units").alias("min_units"),
    )
)
