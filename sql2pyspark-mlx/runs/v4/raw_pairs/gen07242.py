from pyspark.sql import functions as F

matched_keys = (
    df_stores
    .select("region", "id")
    .intersect(
        df_stores
        .filter(F.col("region") == "South")
        .select("region", "id")
    )
)

kept_rows = (
    df_stores
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("activity_month"), "region")
    .agg(
        F.countDistinct("region").alias("distinct_regions"),
        F.sum(F.when(F.col("region") == "North", F.col("sqft")).otherwise(0)).alias("north_sqft"),
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("city") == "Bangkok", 1)), F.lit(0)), 2).alias("sqft_per_bangkok"),
        F.percentile_approx("sqft", 0.95).alias("sqft_p95"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
    )
)
