from pyspark.sql import functions as F

remaining_keys = (
    df_store_sales
    .select("store_id")
    .intersect(
        df_store_sales
        .filter(F.col("revenue") <= 1900)
        .select("store_id")
    )
)

verified = (
    df_store_sales
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .withColumn("activity_month", F.date_trunc("month", "sale_date"))
    .withColumn("quarter_start", F.date_trunc("quarter", "sale_date"))
    .withColumn("week_start", F.date_trunc("week", "sale_date"))
    .rollup("activity_month", "quarter_start", "week_start")
    .agg(
        F.percentile_approx("revenue", 0.95).alias("revenue_p95"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.percentile_approx("revenue", [0.25, 0.5, 0.75]).alias("revenue_quartiles"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.count("*").alias("event_count"),
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.grouping_id().alias("grouping_level"),
    )
)
