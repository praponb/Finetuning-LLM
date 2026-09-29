from pyspark.sql import functions as F

matched_keys = (
    df_stores
    .select("region", "id")
    .subtract(
        df_stores
        .filter(F.col("city").isin(["Chiang Mai", "Bangkok", "London"]))
        .select("region", "id")
    )
)

verified = (
    df_stores
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.date_trunc("QUARTER", "opened_date").alias("quarter_start"),
        F.when(F.col("sqft") >= 13512, "LARGE").otherwise("SMALL").alias("sqft_tier"),
    )
    .agg(
        F.stddev_samp("sqft").alias("sqft_stddev"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.sum(F.when(F.col("region") == "South", F.col("sqft")).otherwise(0)).alias("south_sqft"),
        F.percentile_approx("sqft", 0.95).alias("sqft_p95"),
        F.countDistinct("city").alias("distinct_cities"),
        F.min("sqft").alias("min_sqft"),
    )
    .filter(F.col("sqft_stddev") > 0)
    .orderBy(F.col("south_sqft").desc())
)
