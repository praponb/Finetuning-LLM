from pyspark.sql import functions as F

exploded = (
    df_carts
    .select("id", "coupon_codes", F.explode_outer("attributes").alias("k", "value"))
)

flattened = exploded.select("k", F.explode("coupon_codes").alias("code"))

filtered = (
    flattened
    .filter(F.col("code") == "VIP")
    .withColumn("clean_k", F.trim("k"))
)

cleaned = (
    filtered
    .filter(F.col("k").isNotNull() & F.col("k").isin(["utm_source", "gift"]))
    .withColumn("is_bundle", F.when(F.col("code") == "BUNDLE", 1).otherwise(0))
    .withColumn("k_prefix", F.substring("k", 1, 3))
)

cleaned_carts = (
    cleaned
    .filter(~F.col("code").isin(["VIP", "SPRING10", "WELCOME5"]))
    .withColumn("is_bundle_whole", F.floor("is_bundle"))
)

valid = (
    cleaned_carts
    .filter(F.col("k").isNotNull() & (~F.col("code").eqNullSafe(F.lit("BUNDLE"))))
    .select(
        "*",
        F.greatest("is_bundle_whole", "is_bundle").alias("greatest_is_bundle_whole_is_bundle"),
        F.round("is_bundle_whole", 1).alias("is_bundle_whole_rounded"),
    )
)

result = (
    valid
    .groupBy("code")
    .agg(
        F.percentile_approx("greatest_is_bundle_whole_is_bundle", [0.25, 0.5, 0.75]).alias("greatest_is_bundle_whole_is_bundle_quartiles"),
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("code") == "BUNDLE", F.col("is_bundle_whole")).otherwise(0)).alias("bundle_is_bundle_whole"),
        F.count("*").alias("event_count_2"),
        F.avg(F.when(F.col("code") == "SPRING10", F.col("is_bundle"))).alias("spring10_avg_is_bundle"),
    )
    .orderBy(F.col("code").asc_nulls_first())
)
